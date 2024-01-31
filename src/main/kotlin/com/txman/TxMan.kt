package com.txman

import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.UpdatableRecord
import org.jooq.impl.DAOImpl
import org.jooq.impl.transactionResult
import java.util.LinkedList

typealias ConnConfigFun = (Configuration) -> Configuration
class TxMan(private val configuration: Configuration) {
    private val map = ConcurrentHashMap<CoroutineContext, ArrayDeque<Configuration>>()
    private val commitCallbacksMap = ConcurrentHashMap<CoroutineContext, LinkedList<suspend () -> Unit>>()
    private val commitCallbackPointerStack = ConcurrentHashMap<CoroutineContext, ArrayDeque<Int>>()

    private suspend fun <T> getLambdaFn(lambda: suspend () -> T): suspend (Configuration) -> T {
        return {
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
                }
            }

            result
        }
    }
    suspend fun <T> execute(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val suspendLambda = getLambdaFn(lambda)
        val configuration = configureConnection?.invoke(configuration) ?: configuration
        return configuration.dsl().transactionResult(suspendLambda)
        // TODO: think what to do when error happens while commiting execute commit collabacks will be called even if rollback happens
        // think three level txman how save poin will work
    }

    suspend fun <T> wrap(configureConnection: ConnConfigFun? = null, lambda: suspend () -> T): T {
        val suspendLambda = getLambdaFn(lambda)
        val configuration = configureConnection?.let { configureConnection.invoke(configuration()) } ?: configuration()
        val value = try {
            configuration.dsl().transactionResult(suspendLambda)
        } catch (e: Exception) {
            val context = kotlinx.coroutines.currentCoroutineContext()
            var lastCount = commitCallbackPointerStack[context]?.removeLastOrNull()
            while(lastCount != null && lastCount > 0) {
                executeCommitCallbacks()
                lastCount--
            }
            throw e
        }

        executeCommitCallbacks()
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
            commitCallbacksMap[context] = LinkedList()
        }
        commitCallbacksMap[context]?.addLast(lambda)
        val lastCount = commitCallbackPointerStack[context]?.removeLastOrNull()
        commitCallbackPointerStack[context]?.addLast(lastCount?.plus(1) ?: 1)
    }

    data class Stats(val configurationMapSize: Int, val commitCallbacksMapSize: Int)
    fun statistics() = Stats(map.size, commitCallbacksMap.size)

    private suspend fun pushConfiguration(configuration: Configuration) {
        val context = kotlinx.coroutines.currentCoroutineContext()
        if (!map.containsKey(context)) {
            // Initializing a dequeue of size 3 based on dev team's usage heuristics
            map[context] = ArrayDeque(3)
            commitCallbackPointerStack[context] = ArrayDeque(3)
        }
        map[context]?.addLast(configuration)
        commitCallbackPointerStack[context]?.addLast(0)
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