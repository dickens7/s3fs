package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
)

// S3API 抽象，便于 mock
// 只列出当前需要的方法

type S3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
}

// NewDefaultClient 使用默认 credential chain
func NewDefaultClient(ctx context.Context, region string, opts ...func(*config.LoadOptions) error) (S3API, error) {
	cfg, err := config.LoadDefaultConfig(ctx, append(opts, config.WithRegion(region))...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

// S3Fs 实现 afero.Fs
// 目录通过 key 前缀 + '/' 分隔模拟

type S3Fs struct {
	bucket     string
	client     S3API
	prefix     string // 根前缀，可为空
	contextFn  func() context.Context
	pageSize   int32
	useDirMark bool // 目录是否写一个占位对象 (key/)
}

// Option 配置

type Option func(*S3Fs)

func WithPrefix(p string) Option                     { return func(f *S3Fs) { f.prefix = strings.Trim(p, "/") } }
func WithContextFn(fn func() context.Context) Option { return func(f *S3Fs) { f.contextFn = fn } }
func WithPageSize(n int32) Option                    { return func(f *S3Fs) { f.pageSize = n } }
func WithDirMarker() Option                          { return func(f *S3Fs) { f.useDirMark = true } }

// NewS3Fs 构造
func NewS3Fs(bucket string, client S3API, opts ...Option) *S3Fs {
	fs := &S3Fs{
		bucket:    bucket,
		client:    client,
		contextFn: func() context.Context { return context.Background() },
		pageSize:  1000,
	}
	for _, o := range opts {
		o(fs)
	}
	return fs
}

func (f *S3Fs) ctx() context.Context { return f.contextFn() }

func (f *S3Fs) Name() string { return "s3fs" }

// ---------------- path & key helpers ----------------

func (f *S3Fs) toKey(name string) string {
	clean := filepath.ToSlash(path.Clean(name))
	clean = strings.TrimPrefix(clean, "./")
	if clean == "." || clean == "/" {
		clean = ""
	}
	if f.prefix != "" {
		if clean != "" {
			return f.prefix + "/" + clean
		}
		return f.prefix
	}
	return clean
}

// ---------------- afero.Fs methods ----------------

func (f *S3Fs) Create(name string) (afero.File, error) {
	return f.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}

func (f *S3Fs) Mkdir(name string, perm fs.FileMode) error {
	if strings.Contains(name, "//") {
		return fs.ErrInvalid
	}
	if f.useDirMark {
		key := f.toKey(name)
		if !strings.HasSuffix(key, "/") {
			key += "/"
		}
		_, err := f.client.PutObject(f.ctx(), &s3.PutObjectInput{Bucket: &f.bucket, Key: &key, Body: bytes.NewReader(nil)})
		return err
	}
	return nil // 虚拟目录
}

func (f *S3Fs) MkdirAll(p string, perm fs.FileMode) error { return nil }

func (f *S3Fs) Open(name string) (afero.File, error) { return f.OpenFile(name, os.O_RDONLY, 0) }

func (f *S3Fs) OpenFile(name string, flag int, perm fs.FileMode) (afero.File, error) {
	key := f.toKey(name)
	write := (flag & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREATE | os.O_TRUNC)) != 0

	if write {
		// 不允许以 / 结尾写入
		if strings.HasSuffix(name, "/") {
			return nil, errors.New("cannot write directory path")
		}
		return newS3FileWriter(f, name, key, flag), nil
	}

	// 目录显式打开
	if strings.HasSuffix(name, "/") || name == "." || name == "" || name == "/" {
		return f.openDir(name, key)
	}

	out, err := f.client.GetObject(f.ctx(), &s3.GetObjectInput{Bucket: &f.bucket, Key: &key})
	if err != nil {
		// 如果对象不存在，尝试作为目录
		if !strings.HasSuffix(key, "/") {
			if dirf, derr := f.openDir(name, key); derr == nil {
				return dirf, nil
			}
		}
		return nil, mapNotFound(err)
	}
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	_ = out.Body.Close()
	var clen int64
	if out.ContentLength != nil {
		clen = *out.ContentLength
	}
	return newS3FileReader(f, name, key, data, out.LastModified, clen), nil
}

// openDir 枚举模拟目录的直接子项
func (f *S3Fs) openDir(name, key string) (afero.File, error) {
	if key != "" && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	delim := "/"
	ctx := f.ctx()
	var token *string
	var infos []fs.FileInfo
	for {
		out, err := f.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &f.bucket, Prefix: &key, Delimiter: &delim, ContinuationToken: token})
		if err != nil {
			return nil, mapNotFound(err)
		}
		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			p := strings.TrimPrefix(*cp.Prefix, key)
			p = strings.TrimSuffix(p, "/")
			infos = append(infos, &fileInfo{name: p, dir: true, modTime: time.Now()})
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			if *obj.Key == key {
				continue
			} // 目录 marker
			p := strings.TrimPrefix(*obj.Key, key)
			if strings.Contains(p, "/") {
				continue
			} // 只要直接子项
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			mt := time.Now()
			if obj.LastModified != nil {
				mt = *obj.LastModified
			}
			infos = append(infos, &fileInfo{name: p, size: size, modTime: mt})
		}
		if out.IsTruncated != nil && *out.IsTruncated && out.NextContinuationToken != nil {
			token = out.NextContinuationToken
			continue
		}
		break
	}
	if key != "" && len(infos) == 0 { // 目录不存在
		return nil, fs.ErrNotExist
	}
	return &s3File{fs: f, name: name, key: key, flag: os.O_RDONLY, dir: true, children: infos, modTime: time.Now()}, nil
}

func (f *S3Fs) Remove(name string) error {
	key := f.toKey(name)
	_, err := f.client.DeleteObject(f.ctx(), &s3.DeleteObjectInput{Bucket: &f.bucket, Key: &key})
	return err
}

func (f *S3Fs) RemoveAll(p string) error { return f.Remove(p) }

func (f *S3Fs) Rename(oldname, newname string) error {
	oldKey := f.toKey(oldname)
	newKey := f.toKey(newname)
	copySource := f.bucket + "/" + oldKey
	_, err := f.client.CopyObject(f.ctx(), &s3.CopyObjectInput{Bucket: &f.bucket, Key: &newKey, CopySource: &copySource})
	if err != nil {
		return err
	}
	_, _ = f.client.DeleteObject(f.ctx(), &s3.DeleteObjectInput{Bucket: &f.bucket, Key: &oldKey})
	return nil
}

func (f *S3Fs) Stat(name string) (fs.FileInfo, error) {
	key := f.toKey(name)
	if key == "" {
		return &fileInfo{name: "/", dir: true, size: 0, modTime: time.Now()}, nil
	}
	// try head
	out, err := f.client.HeadObject(f.ctx(), &s3.HeadObjectInput{Bucket: &f.bucket, Key: &key})
	if err == nil {
		var size int64
		if out.ContentLength != nil {
			size = *out.ContentLength
		}
		return &fileInfo{name: filepath.Base(key), size: size, modTime: derefTime(out.LastModified)}, nil
	}
	// maybe directory (prefix exists)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	var one int32 = 1
	list, lerr := f.client.ListObjectsV2(f.ctx(), &s3.ListObjectsV2Input{Bucket: &f.bucket, Prefix: &key, MaxKeys: &one})
	if lerr == nil && (len(list.Contents) > 0 || len(list.CommonPrefixes) > 0) {
		return &fileInfo{name: filepath.Base(strings.TrimSuffix(key, "/")), dir: true, size: 0, modTime: time.Now()}, nil
	}
	return nil, fs.ErrNotExist
}

func (f *S3Fs) Chmod(name string, mode fs.FileMode) error                   { return nil }
func (f *S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error { return nil }

// -------------- 文件实现 --------------

type s3File struct {
	fs       *S3Fs
	name     string // original path
	key      string
	flag     int
	buf      *bytes.Buffer
	closed   bool
	modTime  time.Time
	size     int64
	children []fs.FileInfo // for directories (未实现目录文件对象，这里简化)
	dir      bool
}

func newS3FileWriter(f *S3Fs, name, key string, flag int) *s3File {
	return &s3File{fs: f, name: name, key: key, flag: flag, buf: &bytes.Buffer{}, modTime: time.Now()}
}

func newS3FileReader(f *S3Fs, name, key string, data []byte, mod *time.Time, size int64) *s3File {
	mt := time.Now()
	if mod != nil {
		mt = *mod
	}
	return &s3File{fs: f, name: name, key: key, flag: os.O_RDONLY, buf: bytes.NewBuffer(data), size: int64(len(data)), modTime: mt}
}

func (f *s3File) Name() string { return filepath.Base(f.name) }

func (f *s3File) Stat() (fs.FileInfo, error) {
	return &fileInfo{name: f.Name(), size: int64(f.buf.Len()), modTime: f.modTime}, nil
}

func (f *s3File) Read(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.buf.Read(p)
}

func (f *s3File) Write(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	if (f.flag & (os.O_WRONLY | os.O_RDWR | os.O_APPEND)) == 0 {
		return 0, errors.New("file not opened for write")
	}
	return f.buf.Write(p)
}

func (f *s3File) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if (f.flag & (os.O_WRONLY | os.O_RDWR | os.O_CREATE | os.O_TRUNC | os.O_APPEND)) != 0 {
		body := bytes.NewReader(f.buf.Bytes())
		_, err := f.fs.client.PutObject(f.fs.ctx(), &s3.PutObjectInput{Bucket: &f.fs.bucket, Key: &f.key, Body: body})
		return err
	}
	return nil
}

func (f *s3File) Sync() error               { return nil }
func (f *s3File) Truncate(size int64) error { return errors.New("not implemented") }
func (f *s3File) Readdir(count int) ([]fs.FileInfo, error) {
	if !f.dir {
		return nil, errors.New("not a directory")
	}
	if f.children == nil {
		return []fs.FileInfo{}, io.EOF
	}
	if count <= 0 || count > len(f.children) {
		res := f.children
		f.children = nil
		return res, io.EOF
	}
	res := f.children[:count]
	f.children = f.children[count:]
	if len(f.children) == 0 {
		return res, io.EOF
	}
	return res, nil
}
func (f *s3File) Readdirnames(n int) ([]string, error) {
	infos, err := f.Readdir(n)
	names := make([]string, 0, len(infos))
	for _, i := range infos {
		names = append(names, i.Name())
	}
	return names, err
}
func (f *s3File) WriteAt(p []byte, off int64) (int, error) { return 0, errors.New("not implemented") }
func (f *s3File) ReadAt(p []byte, off int64) (int, error)  { return 0, errors.New("not implemented") }
func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("not implemented")
}
func (f *s3File) Chmod(mode fs.FileMode) error      { return nil }
func (f *s3File) Chown(uid, gid int) error          { return nil }
func (f *s3File) WriteString(s string) (int, error) { return f.Write([]byte(s)) }
func (f *s3File) StatModTime() time.Time            { return f.modTime }
func (f *s3File) Type() fs.FileMode                 { return 0 }
func (f *s3File) Info() (fs.FileInfo, error)        { return f.Stat() }

// fileInfo 实现

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	dir     bool
}

func (fi *fileInfo) Name() string { return fi.name }
func (fi *fileInfo) Size() int64  { return fi.size }
func (fi *fileInfo) Mode() fs.FileMode {
	if fi.dir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.dir }
func (fi *fileInfo) Sys() any           { return nil }

// 辅助
func mapNotFound(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "no such key") {
		return fs.ErrNotExist
	}
	return err
}

func derefTime(t *time.Time) time.Time {
	if t != nil {
		return *t
	}
	return time.Now()
}

// Ensure interface compliance at compile time
// afero.Fs 需要的额外方法: Chown (afero 里 Fs 接口没有 Chown; 但某些包装使用)
func (f *S3Fs) Chown(name string, uid, gid int) error { return nil }

var _ afero.Fs = (*S3Fs)(nil)
var _ afero.File = (*s3File)(nil)

// 暂不实现的高级功能： multipart upload, range read, directory listing 等
// 后续步骤会补充目录列举与更完整的接口
