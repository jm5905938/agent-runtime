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

覆盖内容包括agent完整往返、精确数字和查询副本，event重复接收与内容冲突，接收及action提交顺序，同agent并发领取，完整execution token检查，提交校验失败后state、action和执行进度保持不变，失败重排及attempt历史，action结果原子发布与重复完成，unknown策略及尝试上限，独占session、恢复及关闭门禁，同后端重开后的恢复转换与启动报告副本

套件只直接操作store，不运行runner或handler。用例中的中断记录由公开claim接口产生，随后关闭session并重开，验证的是存储恢复契约，不代表真实进程崩溃

## SQLite接入边界

当前`runtime/sqlite`只完成A批次，`Backend`尚未实现完整`core.RecoveryStore`，不能调用本套件，也不能用返回nil的空实现绕过测试

完整实现接口后，可在SQLite外部测试包采用以下接线。示例以届时保留`sqlite.Open(path)`和`Backend.Close()`为前提，目前不应复制到编译中的测试文件

```go
func TestSQLiteRecoveryStore(t *testing.T) {
    storetest.Run(t, func(t *testing.T) core.RecoveryStore {
        backend, err := sqlite.Open(filepath.Join(t.TempDir(), "store.db"))
        if err != nil {
            t.Fatal(err)
        }
        t.Cleanup(func() {
            if err := backend.Close(); err != nil {
                t.Error(err)
            }
        })
        return backend
    })
}
```

该示例需要导入`testing`、`path/filepath`、`agent-runtime/core`、`agent-runtime/sqlite`和`agent-runtime/storetest`，每个子用例必须使用自己的临时数据库，不能使用开发者现有数据库

公共套件通过不证明以下SQLite专属行为，仍须在SQLite包及集成测试中另行验收

- 连接池重建后的PRAGMA、schema迁移、旧库兼容性及数据库损坏校验
- SQL事务中间写入失败后的真实回滚，公共套件中的非法输入仅证明可观察的原子性，不保证已执行过部分SQL写入
- 不同后端对象、路径别名和不同进程之间的文件所有权
- 子进程强杀、重新创建后端对象、WAL及机器断电后的持久化
- 外部handler调用与保存结果之间的故障窗口、实际副作用次数和runtime关闭等待

在`runtime`目录运行

```sh
GOCACHE=/tmp/agent-runtime-p5-go-build GOPROXY=off go test -race -count=1 ./storetest
```
