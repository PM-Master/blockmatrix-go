package blockmatrix

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"io/ioutil"
	"os"
	"testing"
)

var db *leveldb.DB

func TestMain(m *testing.M) {
	var err error

	dir, err := ioutil.TempDir("./db", "test_db")
	if err != nil {
		os.Exit(1)
	}
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
	}(dir)

	db, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		os.Exit(3)
	}

	m.Run()
}

func TestRowBlockNumbers(t *testing.T) {
	bm, err := New(db)
	require.NoError(t, err)

	err = createTestBlocks(bm, 5)
	require.NoError(t, err)
	actual, err := bm.RowBlockNumbers(2)
	require.NoError(t, err)
	require.Equal(t, []int{4, 6}, actual)

	err = createTestBlocks(bm, 20)
	require.NoError(t, err)
	actual, err = bm.RowBlockNumbers(0)
	require.NoError(t, err)
	require.Equal(t, []int{1, 3, 7, 13, 21}, actual)
	actual, err = bm.RowBlockNumbers(3)
	require.NoError(t, err)
	require.Equal(t, []int{8, 10, 12, 19, 27}, actual)
}

func TestColumnBlockNumbers(t *testing.T) {
	bm, err := New(db)
	require.NoError(t, err)

	err = createTestBlocks(bm, 5)
	require.NoError(t, err)
	actual, err := bm.ColumnBlockNumbers(1)
	require.NoError(t, err)
	require.Equal(t, []int{1, 6}, actual)

	err = createTestBlocks(bm, 20)
	require.NoError(t, err)
	actual, err = bm.ColumnBlockNumbers(0)
	require.NoError(t, err)
	require.Equal(t, []int{2, 4, 8, 14, 22}, actual)
	actual, err = bm.ColumnBlockNumbers(3)
	require.NoError(t, err)
	require.Equal(t, []int{7, 9, 11, 20, 28}, actual)
}

func createTestBlocks(bm *BlockMatrix, num int) error {
	for i := 0; i < num; i++ {
		err := bm.AddBlock(fmt.Sprintf("key%d", i), []byte{byte(i)})
		if err != nil {
			return err
		}
	}

	return nil
}

func TestPrintBlockMatrixData(t *testing.T) {
	bm, err := New(db)
	require.NoError(t, err)

	err = bm.AddBlock("key1", []byte{1})
	require.NoError(t, err)
	err = bm.AddBlock("key2", []byte{2})
	require.NoError(t, err)
	err = bm.AddBlock("key3", []byte{3})
	require.NoError(t, err)
	err = bm.AddBlock("key4", []byte{4})
	require.NoError(t, err)
	err = bm.AddBlock("key5", []byte{5})
	require.NoError(t, err)
	err = bm.AddBlock("key6", []byte{6})
	require.NoError(t, err)
	err = bm.AddBlock("key7", []byte{7})
	require.NoError(t, err)
	err = bm.AddBlock("key8", []byte{8})
	require.NoError(t, err)
	err = bm.AddBlock("key9", []byte{9})
	require.NoError(t, err)
	err = bm.AddBlock("key10", []byte{10})
	require.NoError(t, err)
	err = bm.AddBlock("key11", []byte{11})
	require.NoError(t, err)
	err = bm.AddBlock("key12", []byte{12})
	require.NoError(t, err)
	err = bm.AddBlock("key13", []byte{13})
	require.NoError(t, err)
	err = bm.AddBlock("key14", []byte{14})
	require.NoError(t, err)
	err = bm.AddBlock("key15", []byte{15})
	require.NoError(t, err)
	err = bm.AddBlock("key16", []byte{16})
	require.NoError(t, err)
	err = bm.AddBlock("key17", []byte{17})
	require.NoError(t, err)
	err = bm.AddBlock("key18", []byte{18})
	require.NoError(t, err)
	err = bm.AddBlock("key19", []byte{19})
	require.NoError(t, err)
	err = bm.AddBlock("key20", []byte{20})
	require.NoError(t, err)
	err = bm.AddBlock("key21", []byte{21})
	require.NoError(t, err)
	err = bm.AddBlock("key22", []byte{22})
	require.NoError(t, err)

	err = bm.PrintBlockMatrixData()
	require.NoError(t, err)
}
