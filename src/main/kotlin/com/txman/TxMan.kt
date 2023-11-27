package com.txman

import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.UpdatableRecord
import org.jooq.impl.DAOImpl
import org.jooq.impl.transactionResult

typealias ConnConfigFun = (Configuration) -> Configuration
class TxMan(private val configuration: Configuration) {
    private val contextMap = ConcurrentHashMap<CoroutineContext, ArrayDeque<Configuration>>()
    private val stringMap = ConcurrentHashMap<String, ArrayDeque<Configuration>>()
    private val commitCallbacksMap = ConcurrentHashMap<CoroutineContext, ArrayDeque<suspend () -> Unit>>()

    suspend fun <T> execute(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val random = Math.random().toString()
        val suspendLambda: suspend (c: Configuration) -> T = {
            var pushSuccess = false
            val result: T
            var commitSuccess = false
            try {
                pushConfiguration(it, random)
                pushSuccess = true
                result = lambda()
                commitSuccess = true
            } finally {
                if (pushSuccess) {
                    popConfiguration(random)
                    if (commitSuccess) {
                        executeCommitCallbacks()
                    }
                }
            }

            result
        }

    val configuration = configureConnection?.let { configureConnection.invoke(configuration(random)) }
        ?: configuration(random)

    return configuration.dsl().transactionResult(suspendLambda)
    }

    suspend fun <T> wrap(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val suspendLambda: suspend (c: Configuration) -> T = {
            var pushSuccess = false
            val result: T
            var commitSuccess = false
            try {
                pushConfiguration(it)
                pushSuccess = true
                result = lambda()
                commitSuccess = true
            } finally {
                if (pushSuccess) {
                    popConfiguration()
                    if(commitSuccess) {
                        executeCommitCallbacks()
                    }
                }
            }

            result
        }

        val configuration = configureConnection?.let { configureConnection.invoke(configuration()) } ?: configuration()

        return configuration.dsl().transactionResult(suspendLambda)
    }

    suspend fun dsl(): DSLContext {
        return configuration().dsl()
    }

    suspend fun <R : UpdatableRecord<R>, P, T> getDao(
        table: Table<R>,
        type: Class<P>,
        idFun: (P) -> T
    ): DAOImpl<R, P, T> {
        return TxManDAOImpl(table, type, configuration(), idFun)
    }

    suspend fun configuration(mapKey: String? = null): Configuration {
        if (mapKey != null) {
            val stack = stringMap[mapKey]
            if (stack.isNullOrEmpty()) {
                return configuration
            }
            return stack.last()
        } else {
            val stack = contextMap[kotlinx.coroutines.currentCoroutineContext()]
            if (stack.isNullOrEmpty()) {
                return configuration
            }
            return stack.last()
        }
    }

    suspend fun onCommit(lambda: suspend () -> Unit) {
        val context = kotlinx.coroutines.currentCoroutineContext()
        if (!commitCallbacksMap.containsKey(context)) {
            // Initializing a dequeue of size 1 as commitCallbacks is a sparsely used feature
            commitCallbacksMap[context] = ArrayDeque(1)
        }
        commitCallbacksMap[context]?.addLast(lambda)
    }

    data class Stats(val configurationMapSize: Int, val commitCallbacksMapSize: Int)
    fun statistics() = Stats(contextMap.size, commitCallbacksMap.size)

    private suspend fun pushConfiguration(configuration: Configuration, mapKey: String? = null) {
        if (mapKey != null) {
            if (!stringMap.containsKey(mapKey)) {
                // Initializing a dequeue of size 3 based on dev team's usage heuristics
                stringMap[mapKey] = ArrayDeque(3)
            }
            stringMap[mapKey]?.addLast(configuration)
        } else {
            val contextKey = kotlinx.coroutines.currentCoroutineContext()
            if (!contextMap.containsKey(contextKey)) {
                // Initializing a dequeue of size 3 based on dev team's usage heuristics
                contextMap[contextKey] = ArrayDeque(3)
            }
            contextMap[contextKey]?.addLast(configuration)
        }
    }

    private suspend fun popConfiguration(mapKey: String? = null) {
        if (mapKey != null) {
            val stack = stringMap[mapKey]
            if (stack.isNullOrEmpty()) {
                println("[ERROR]: Trying to pop from a non-existent key. Potential bug!")
                return
            } else if (stack.size == 1) {
                stringMap.remove(mapKey)
            } else {
                stack.removeLast()
            }
        }
        else {
        val context = kotlinx.coroutines.currentCoroutineContext()
        val stack = contextMap[context]
        if (stack.isNullOrEmpty()) {
            println("[ERROR]: Trying to pop from a non-existent key. Potential bug!")
            return
        } else if (stack.size == 1) {
            contextMap.remove(context)
        } else {
            stack.removeLast()
        }}
    }

    private suspend fun executeCommitCallbacks() {
        val context = kotlinx.coroutines.currentCoroutineContext()
        val callbacks = commitCallbacksMap[context]
        if (callbacks.isNullOrEmpty()) {
            return
        }

        if (contextMap[context].isNullOrEmpty()) {
            // This implies a COMMIT and not a SAVEPOINT. Hence, executing callbacks.
            callbacks.forEach { it() }
        }
    }
}

class TxManDAOImpl<R : UpdatableRecord<R>, P, T>(
    table: Table<R>,
    type: Class<P>,
    configuration: Configuration,
    val idFun: (P) -> T
) : DAOImpl<R, P, T>(table, type, configuration) {
    override fun getId(pojo: P): T {
        return idFun(pojo)
    }
}
