# TxMan

Transaction manager for Kotlin + Jooq.

Tested with Postgres

[![](https://jitpack.io/v/hwslabs/txman.svg)](https://jitpack.io/#hwslabs/txman)

# Usage

## Initialize

```kotlin
val configuration: org.jooq.Configuration
val txMan = Txman(configuration)
```

## wrap()
Wrap a block with `wrap()` method to execute all statements
present across multiple classes within the same database transaction.

This is achieved by storing the derived configuration corresponding to each coroutineContext
in a Map and using appropriate configuration based on the caller.

**A.kt**
```
val result = txMan.wrap {
    repositoryA.insert(...)
    return foo()
}
```
**B.kt**
```
fun foo() {
    return repositoryB.select(...)
} 
```

## configuration()
Returns an `org.jooq.Configuration` object corresponding to the current active transaction
based on the enclosing `wrap`. If this method is called outside the wrap,
it returns the default configuration provided when initializing TxMan.

## getDao()

Returns instance of `TxManDAOImpl` initialized with the appropriate configuration based on enclosing wrap.

```kotlin
val idFun = fun (organization: Organizations): String {
        return organization.id
}

// Using auto generated DAO methods
val result = txMan.wrap {
    val dao = txMan.getDao(Record, Pojo::class.java, idFun)
    return dao.fetchById("idValue")
}

// Building custom queries
val result2 = txMan.wrap {
    val dao = txMan.getDao(Record, Pojo::class.java, idFun)
    return dao.select(dao.ctx()).update(dao.table).set(/*...*/)
}
```
