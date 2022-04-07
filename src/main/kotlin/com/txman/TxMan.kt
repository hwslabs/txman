package com.txman

import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.UpdatableRecord
import org.jooq.impl.DAOImpl
import org.jooq.impl.transactionResult

class TxMan(private val configuration: Configuration) {
    private val map = ConcurrentHashMap<CoroutineContext, ArrayDeque<Configuration>>()

    suspend fun <T> wrap(lambda: suspend () -> T): T {
        val suspendLambda: suspend (c: Configuration) -> T = {
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

        return dsl().transactionResult(suspendLambda)
    }

    suspend fun dsl(): DSLContext {
        return getCurrentConfiguration().dsl()
    }

    suspend fun configuration(): Configuration {
        return getCurrentConfiguration()
    }

    suspend fun <R : UpdatableRecord<R>, P, T> getDao(
        table: Table<R>,
        type: Class<P>,
        idFun: (P) -> T
    ): DAOImpl<R, P, T> {
        return TxManDAOImpl(table, type, configuration(), idFun)
    }

    private suspend fun getCurrentConfiguration(): Configuration {
        val stack = map[kotlinx.coroutines.currentCoroutineContext()]
        if (stack.isNullOrEmpty()) {
            return configuration
        }
        return stack.last()
    }

    private suspend fun pushConfiguration(configuration: Configuration) {
        val context = kotlinx.coroutines.currentCoroutineContext()
        if (!map.containsKey(context)) {
            map[context] = ArrayDeque()
        }
        map[context]!!.addLast(configuration)
    }

    private suspend fun popConfiguration() {
        val stack = map[kotlinx.coroutines.currentCoroutineContext()]
        if (stack.isNullOrEmpty()) {
            println("[ERROR]: Trying to pop from a non-existent key. Potential bug!")
            return
        }
        stack.removeLast()
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
