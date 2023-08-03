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

## onCommit()
Use `onCommit()` to configure one or more callbacks which will be executed upon successful completion of the transaction.

**A.kt**
```kotlin
txMan.wrap {
    repositoryA.insert(/*...*/)
    txMan.onCommit {
        println("Inserted into repositoryA")
    }
    foo()
    println("Wrap completed")
}
```
**B.kt**
```kotlin
fun foo() {
    repositoryB.select(/*...*/)
    txMan.onCommit {
        println("Selected from repositoryB")
    }
    return
} 
```

**Output of executing A.kt**
```shell
Wrap completed
Inserted into repositoryA
Selected from repositoryB
```

This even works with nested txman blocks.

**C.kt**
```kotlin
txMan.wrap {
    repositoryA.insert(/*...*/)
    txMan.onCommit {
        println("Outer before")
    }
    txMan.wrap {
        repositoryB.select(/*...*/)
        txMan.onCommit {
            println("Inner")
        }
    }
    txMan.onCommit {
        println("Outer after")
    }
}
```
**Output of executing C.kt**
```shell
Outer before
Inner
Outer after
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
