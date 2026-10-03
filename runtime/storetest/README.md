# storetest公共存储契约

`storetest.Run`通过`core.RecoveryStore`、`core.RecoverySession`和`core.StateStore`的公开接口构造数据并验证行为，不读取内存后端的私有map，也不依赖具体数据库

每个子用例调用一次工厂，工厂必须返回空的、与其他用例隔离的后端。工厂负责用`t.Cleanup`关闭数据库等底层资源，套件在工厂返回后登记session清理，因此先关闭session，再清理后端。恢复用例会在同一后端上关闭并重新打开session，工厂不能在关闭session时删除数据

内存后端已在外部测试包中接入

```go
func TestMemoryRecoveryStore(t *testing.T) {
    storetest.Run(t, func(t *testing.T) core.RecoveryStore {
        return core.NewMemoryRecoveryStore()
    })
}
```

覆盖内容包括agent完整往返、精确数字和查询副本，完整event校验、重复接收与内容冲突，接收及action提交顺序，同agent并发领取，完整execution token检查，提交校验失败后state、action和执行进度保持不变，状态版本耗尽，失败重排及超过9次的attempt历史，action元数据校验及有效字段原文往返，action结果原子发布、重复完成和已完成后旧token拒绝，大整数、小数及指数的精确比较，failure合法性，unknown策略及尝试上限，独占session、恢复及关闭门禁，同后端多次重开后的恢复转换、已有unknown报告与启动报告副本

套件只直接操作store，不运行runner或handler。用例中的中断记录由公开claim接口产生，随后关闭session并重开，验证的是存储恢复契约，不代表真实进程崩溃

## SQLite接入边界

SQLite已在`runtime/sqlite/contract_test.go`中通过`testRecoveryStore`适配器接入同一套公共契约。`Backend.OpenSession`返回具体的`*sqlite.Session`，适配器将返回类型转换为`core.RecoverySession`，因此不能直接把`*sqlite.Backend`作为`core.RecoveryStore`传给工厂

当前接线位于SQLite包内，主要代码如下

```go
type testRecoveryStore struct {
    *Backend
}

func (s testRecoveryStore) OpenSession(ctx context.Context) (core.RecoverySession, error) {
    session, err := s.Backend.OpenSession(ctx)
    if err != nil {
        return nil, err
    }
    return session, nil
}

func TestSQLiteContract(t *testing.T) {
    storetest.Run(t, func(t *testing.T) core.RecoveryStore {
        backend, err := Open(filepath.Join(t.TempDir(), "store.db"))
        if err != nil {
            t.Fatal(err)
        }
        t.Cleanup(func() {
            _ = backend.Close()
        })
        return testRecoveryStore{backend}
    })
}
```

该测试导入`context`、`testing`、`path/filepath`、`agent-runtime/core`和`agent-runtime/storetest`，每个子用例使用自己的临时数据库，不使用开发者现有数据库

SQLite专属测试另行覆盖恢复前的数据损坏及关联不一致、故障注入造成的当前agent版本漂移、完整uint64边界，以及真正发生部分SQL写入后失败的事务回滚。这些故障不能仅通过公共接口正常串行构造，故不放入公共套件，所有数据库和故障注入均使用临时测试数据

公共套件只验证存储契约，不启动正式CLI。CLI的SQLite接线、真实进程持久化、独占锁和强制终止后的恢复由`runtime/cmd/agent-runtime/backend_sqlite_test.go`另行验证，公共存储测试通过不能作为完整持久化运行闭环已完成的证明

公共套件通过也不单独证明以下SQLite专属行为，仍须在SQLite包及集成测试中另行验收

- 连接池重建后的PRAGMA、schema迁移、旧库兼容性及数据库损坏校验
- SQL事务中间写入失败后的真实回滚，公共套件中的非法输入仅证明可观察的原子性，不保证已执行过部分SQL写入
- 不同后端对象、路径别名和不同进程之间的文件所有权
- 子进程强杀、重新创建后端对象、WAL及机器断电后的持久化
- 外部handler调用与保存结果之间的故障窗口、实际副作用次数和runtime关闭等待

在`runtime`目录运行

```sh
GOCACHE=/tmp/agent-runtime-p5-go-build GOPROXY=off go test -race -count=1 ./storetest ./sqlite
```
