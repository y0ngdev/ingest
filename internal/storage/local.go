package storage

import "os"

type Local struct {
	dir string
}

func New(dir string) (*Local, error) {

	if _, err := os.Stat(dir); err != nil {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}

	}
	return &Local{dir: dir}, nil
}

func (l *Local) Store(ctx context.Context, key string, r io.Reader) error 


}
