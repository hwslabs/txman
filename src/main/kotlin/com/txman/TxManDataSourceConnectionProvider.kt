package com.txman

import java.sql.Connection
import javax.sql.DataSource
import org.jooq.impl.DataSourceConnectionProvider

typealias ConnectionConfigFun = (Connection) -> Connection

class TxManDataSourceConnectionProvider(
    dataSource: DataSource,
    private val configFn: ConnectionConfigFun? = null
) : DataSourceConnectionProvider(dataSource) {

    override fun acquire(): Connection {
        return super.acquire().apply { configFn?.invoke(this) }
    }
}