# S3FS (afero S3 实现)

基于 AWS SDK v2 的 `afero.Fs` 适配器，提供用 `afero` 统一访问 S3 Bucket 的能力。

## 特性

- 纯 Go，依赖 AWS SDK v2
- 以 key 前缀模拟目录，支持 `Open("dir/")` + `Readdir`
- 实现核心文件操作：Create / Open / Stat / Rename / Remove / RemoveAll / Mkdir / MkdirAll
- 文件写入为内存缓冲，`Close()` 时一次性 `PutObject`
- 目录列举使用 `ListObjectsV2` + `Delimiter`，只返回直接子节点
- 目录递归复制：`CopyDir(src, dst)`

## 尚未实现 / TODO

- Seek / ReadAt / WriteAt / Truncate（随机访问）
- Multipart Upload（大文件优化）
- Range 读取（按需流式读取）
- Metadata / ACL / SSE 等高级属性

## 安装

```bash
go get github.com/dickens7/s3fs
```

## 快速开始

```go
package main

import (
 "context"
 "fmt"
 "github.com/spf13/afero"
 s3fs "github.com/dickens7/s3fs"
)

func main() {
 // 初始化 AWS S3 Client (自动走默认凭证链)
 cli, err := s3fs.NewDefaultClient(context.Background(), "ap-northeast-1")
 if err != nil { panic(err) }

 fs := s3fs.NewS3Fs("my-bucket", cli, s3fs.WithPrefix("root"))

 // 写文件
 f, _ := fs.Create("demo/hello.txt")
 f.Write([]byte("Hello S3"))
 f.Close()

 // 读文件
 rf, _ := fs.Open("demo/hello.txt")
 data, _ := afero.ReadAll(rf)
 fmt.Println(string(data))
 rf.Close()

 // 列目录
 dh, _ := fs.Open("demo/")
 entries, _ := dh.Readdir(0)
 for _, e := range entries { fmt.Println(e.Name(), e.IsDir()) }
 dh.Close()

 // 复制整个目录 (demo -> demo_copy)
 if err := fs.CopyDir("demo", "demo_copy"); err != nil { panic(err) }
}
```

## 测试

项目内含一个内存 Mock 的 S3 客户端用于单元测试：

```bash
go test ./...
```

## 设计说明

- 目录：S3 无真正目录，使用前缀 + Delimiter 模拟；可选写入 `dir/` 空对象作为 marker（`WithDirMarker()`）。
- 写入策略：全部写入内存缓冲，`Close` 时统一上传；适合中小文件场景。
- 性能：当前未做多 part / streaming 优化，大文件（>5MB/50MB）建议后续扩展。

## License

MIT
