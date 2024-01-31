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
    private val map = ConcurrentHashMap<CoroutineContext, ArrayDeque<Configuration>>()
    private val commitCallbacksMap = ConcurrentHashMap<CoroutineContext, ArrayDeque<suspend () -> Unit>>()

    private suspend fun <T> getLambdaFn(lambda: suspend () -> T): suspend (Configuration) -> T {
        return {
            var pushSuccess = false
            val result: T
            try {
                pushConfiguration(it)
                pushSuccess = true
                result = lambda()
            } finally {
                if (pushSuccess) {
                    popConfiguration()
                }
            }

            result
        }
    }
    suspend fun <T> execute(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val suspendLambda = getLambdaFn(lambda)
        val configuration = configureConnection?.invoke(configuration) ?: configuration
        return configuration.dsl().transactionResult(suspendLambda)
    }

    suspend fun <T> wrap(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val suspendLambda = getLambdaFn(lambda)
        val context = kotlinx.coroutines.currentCoroutineContext()
        val configuration = configureConnection?.let { configureConnection.invoke(configuration()) } ?: configuration()
        val value = try {
            val response = configuration.dsl().transactionResult(suspendLambda)
            executeCommitCallbacks()
            response
        } finally {
            if (map[context].isNullOrEmpty()) {
                // This implies a COMMIT and not a SAVEPOINT. Hence, removing callbacks
                commitCallbacksMap.remove(context)
            }
        }
        return value
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

    suspend fun configuration(): Configuration {
        val stack = map[kotlinx.coroutines.currentCoroutineContext()]
        if (stack.isNullOrEmpty()) {
            return configuration
        }
        return stack.last()
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
    fun statistics() = Stats(map.size, commitCallbacksMap.size)

    private suspend fun pushConfiguration(configuration: Configuration) {
        val context = kotlinx.coroutines.currentCoroutineContext()
        if (!map.containsKey(context)) {
            // Initializing a dequeue of size 3 based on dev team's usage heuristics
            map[context] = ArrayDeque(3)
        }
        map[context]?.addLast(configuration)
    }

    private suspend fun popConfiguration() {
        val context = kotlinx.coroutines.currentCoroutineContext()
        val stack = map[context]
        if (stack.isNullOrEmpty()) {
            println("[ERROR]: Trying to pop from a non-existent key. Potential bug!")
            return
        } else if (stack.size == 1) {
            map.remove(context)
        } else {
            stack.removeLast()
        }
    }

    private suspend fun executeCommitCallbacks() {
        val context = kotlinx.coroutines.currentCoroutineContext()
        val callbacks = commitCallbacksMap[context]
        if (callbacks.isNullOrEmpty()) {
            return
        }

        if (map[context].isNullOrEmpty()) {
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
