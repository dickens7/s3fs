package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type memObj struct {
	data []byte
	mod  time.Time
}

type mockClient struct {
	bucket string
	objs   map[string]*memObj
}

func newMock(bucket string) *mockClient {
	return &mockClient{bucket: bucket, objs: map[string]*memObj{}}
}

func (m *mockClient) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if o, ok := m.objs[*in.Key]; ok {
		return &s3.GetObjectOutput{Body: nopCloser{bytes.NewReader(o.data)}, LastModified: &o.mod, ContentLength: &[]int64{int64(len(o.data))}[0]}, nil
	}
	return nil, errors.New("NotFound")
}
func (m *mockClient) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	buf := new(bytes.Buffer)
	if in.Body != nil {
		_, _ = buf.ReadFrom(in.Body)
	}
	m.objs[*in.Key] = &memObj{data: buf.Bytes(), mod: time.Now()}
	return &s3.PutObjectOutput{}, nil
}
func (m *mockClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if o, ok := m.objs[*in.Key]; ok {
		cl := int64(len(o.data))
		return &s3.HeadObjectOutput{ContentLength: &cl, LastModified: &o.mod}, nil
	}
	return nil, errors.New("NotFound")
}
func (m *mockClient) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	delete(m.objs, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockClient) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := ""
	if in.Prefix != nil {
		prefix = *in.Prefix
	}
	var contents []types.Object
	var commonPrefixes []types.CommonPrefix
	seenDirs := map[string]struct{}{}
	for k, v := range m.objs {
		if !bytes.HasPrefix([]byte(k), []byte(prefix)) {
			continue
		}
		rest := k[len(prefix):]
		if idx := bytes.IndexByte([]byte(rest), '/'); idx >= 0 {
			d := rest[:idx+1]
			if _, ok := seenDirs[d]; !ok {
				cp := types.CommonPrefix{Prefix: awsString(prefix + d)}
				commonPrefixes = append(commonPrefixes, cp)
				seenDirs[d] = struct{}{}
			}
			continue
		}
		cl := int64(len(v.data))
		lm := v.mod
		contents = append(contents, types.Object{Key: awsString(k), Size: &cl, LastModified: &lm})
	}
	out := &s3.ListObjectsV2Output{}
	out.Contents = append(out.Contents, contents...)
	out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixes...)
	return out, nil
}
func (m *mockClient) CopyObject(ctx context.Context, in *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	parts := bytes.Split([]byte(*in.CopySource), []byte{'/'})
	if len(parts) < 2 {
		return nil, errors.New("invalid source")
	}
	key := string(bytes.Join(parts[1:], []byte{'/'}))
	if o, ok := m.objs[key]; ok {
		m.objs[*in.Key] = &memObj{data: append([]byte(nil), o.data...), mod: time.Now()}
		return &s3.CopyObjectOutput{}, nil
	}
	return nil, errors.New("NotFound")
}

type nopCloser struct{ *bytes.Reader }

func (n nopCloser) Close() error { return nil }

// helper
func awsString(s string) *string { return &s }

func TestBasicCRUD(t *testing.T) {
	mc := newMock("b1")
	fssystem := NewS3Fs("b1", mc)
	f, err := fssystem.Create("dir1/hello.txt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = f.Write([]byte("hello"))
	_ = f.Close()

	st, err := fssystem.Stat("dir1/hello.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Size() != 5 {
		t.Fatalf("size want 5 got %d", st.Size())
	}

	f2, err := fssystem.Open("dir1/hello.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	buf := make([]byte, 10)
	n, _ := f2.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Fatalf("content mismatch: %s", string(buf[:n]))
	}
	_ = f2.Close()

	if err := fssystem.Rename("dir1/hello.txt", "dir1/hi.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := fssystem.Stat("dir1/hello.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old still exists")
	}
}

func TestListDir(t *testing.T) {
	mc := newMock("b1")
	fsys := NewS3Fs("b1", mc)
	// prepare
	w1, _ := fsys.Create("a/b/c.txt")
	w1.Write([]byte("x"))
	w1.Close()
	w2, _ := fsys.Create("a/d.txt")
	w2.Write([]byte("y"))
	w2.Close()
	// 显式创建目录标记，确保 open("a/") 时能列举
	fsys.Mkdir("a", 0o755)

	dh, err := fsys.Open("a/")
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	entries, _ := dh.Readdir(0)
	if len(entries) != 2 {
		t.Fatalf("want 2 entries got %d", len(entries))
	}
	_ = dh.Close()
}
