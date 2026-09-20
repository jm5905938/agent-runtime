# agent-runtime

以目标为核心、支持长期运行的Agent运行环境。当前Go实现完成内存Echo闭环及P1恢复契约：代码版本绑定、已有实例加载、JSON数据边界、执行尝试记录和存储事务接口。

运行模块位于 `runtime/`

```sh
cd runtime
go test -race -count=1 ./...
go vet ./...
CGO_ENABLED=0 go build ./...
```
调用方提供的已有记录&sqlite驱动已固定
