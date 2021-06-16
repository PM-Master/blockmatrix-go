package blockmatrix

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"os"
	"reflect"
	"strconv"
)

type (
	// BlockMatrix implementation that stores blocks in a leveldb key-value database
	BlockMatrix struct {
		db *leveldb.DB
	}

	// BlockMatrixInfo stores information about the block matrix
	BlockMatrixInfo struct {
		// Size of the block matrix (dimension)
		Size int `json:"size"`
		// BlockCount is the number of blocks in the block matrix
		BlockCount int `json:"block_count"`
		// Rows stores the hashes of each row in the block matrix
		Rows [][]byte `json:"rows"`
		// Cols stores the hashes of each column in the block matrix
		Cols [][]byte `json:"cols"`
	}
)

var (
	InfoKey = []byte(fmt.Sprint("info"))
)

// New creates a new block matrix with the given leveldb database.  If the database does not yet have a block matrix,
// the block matrix info entry is created for an empty block matrix.  An empty block matrix has a size of 1.
func New(db *leveldb.DB) (*BlockMatrix, error) {
	if ok, err := db.Has(InfoKey, nil); err != nil {
		return nil, fmt.Errorf("error checking if database has block matrix info")
	} else if !ok {
		if err = initInfo(db); err != nil {
			return nil, fmt.Errorf("error initializing block matrix info %w", err)
		}

		return &BlockMatrix{db: db}, nil
	}

	return &BlockMatrix{db: db}, nil
}

func initInfo(db *leveldb.DB) error {
	info := &BlockMatrixInfo{
		Size: 1,
		Rows: make([][]byte, 1),
		Cols: make([][]byte, 1),
	}

	var (
		bytes []byte
		err   error
	)

	if bytes, err = json.Marshal(info); err != nil {
		return fmt.Errorf("error marshaling block matrix info: %w", err)
	}

	if err = db.Put([]byte("info"), bytes, nil); err != nil {
		return fmt.Errorf("error putting block matrix info bytes: %w", err)
	}

	return nil
}

// Size computes the size of a block matrix with the given block count.  To find the size of the block matrix square root
// the block count and round up.  It's possible the computed size does not have enough available blocks and in this case,
// the size is incremented once to fit all blocks.
func (b *BlockMatrix) Size(blockCount int) int {
	// calculate matrix size which is sqrt(blockCount) rounded up
	size := int(math.Ceil(math.Sqrt(float64(blockCount))))
	// if the number of available blocks (size^2 - size) is less than the block count increase the size by 1
	if size*size-size < blockCount {
		size++
	}

	return size
}

// AddBlock adds a block to the block matrix with the given key and data.  A block effectively has two entries in the
// key value database: key-> blockNumber, blockNumber -> Block.
func (b *BlockMatrix) AddBlock(key string, data []byte) error {
	info, err := b.GetBlockMatrixInfo()
	if err != nil {
		return err
	}

	// increment block counter
	info.BlockCount++

	// check if the block count causes the size to increase
	newSize := b.Size(info.BlockCount)
	if newSize > info.Size {
		if err = b.updateBlockMatrixSize(info, newSize); err != nil {
			return err
		}
	}

	// serialize block number to put in to db
	blockNum := info.BlockCount
	blockNumBytes := []byte(strconv.Itoa(blockNum))

	// construct block
	block := NewBlock(data)

	// serialize block
	bytes, err := json.Marshal(block)
	if err != nil {
		return err
	}

	// put key -> blockNum
	if err = b.db.Put([]byte(key), blockNumBytes, nil); err != nil {
		return err
	}

	// put blockNum -> block
	if err = b.db.Put(blockNumBytes, bytes, nil); err != nil {
		return err
	}

	// update row and col hashes
	return b.updateBlockMatrixInfo(info, blockNum)
}

func (b *BlockMatrix) updateBlockMatrixInfo(info *BlockMatrixInfo, blockNum int) error {
	row, col := b.locateBlock(blockNum)

	var err error

	// calculate row hash
	info.Rows[row], err = b.calculateRowHash(row, info.BlockCount)
	if err != nil {
		return err
	}

	// calculate col hash
	info.Cols[col], err = b.calculateColumnHash(col, info.BlockCount)
	if err != nil {
		return err
	}

	var bytes []byte
	if bytes, err = json.Marshal(info); err != nil {
		return err
	}

	return b.db.Put([]byte("info"), bytes, nil)
}

// GetBlock returns the block associated with the given key.
func (b *BlockMatrix) GetBlock(key string) (*Block, error) {
	bytes, err := b.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	if bytes, err = b.db.Get(bytes, nil); err != nil {
		return nil, err
	}

	block := &Block{}
	if err = json.Unmarshal(bytes, block); err != nil {
		return nil, err
	}

	return block, nil
}

// GetBlockByNumber returns the block with the given block number.
func (b *BlockMatrix) GetBlockByNumber(num int) (*Block, error) {
	bytes, err := b.db.Get([]byte(fmt.Sprint(num)), nil)
	if err != nil {
		return nil, err
	}

	block := &Block{}
	if err = json.Unmarshal(bytes, block); err != nil {
		return nil, err
	}

	return block, nil
}

// BlockNumber returns the block number of the given key.
func (b *BlockMatrix) BlockNumber(key string) (int, error) {
	bytes, err := b.db.Get([]byte(key), nil)
	if err != nil {
		return -1, err
	}

	num, err := strconv.Atoi(string(bytes[0]))
	if err != nil {
		return -1, err
	}

	return num, nil
}

// EraseBlock erases the data from the block associated with the given key.
func (b *BlockMatrix) EraseBlock(key string) error {
	blockNum, err := b.BlockNumber(key)
	if err != nil {
		return err
	}

	// delete key
	if err = b.db.Delete([]byte(key), nil); err != nil {
		return err
	}

	// erase block
	bytes, err := json.Marshal(EmptyBlock())
	if err != nil {
		return err
	}

	if err = b.db.Put([]byte(fmt.Sprint(blockNum)), bytes, nil); err != nil {
		return err
	}

	info, err := b.GetBlockMatrixInfo()
	if err != nil {
		return err
	}

	oldRowHashes := make([][]byte, len(info.Rows))
	oldColHashes := make([][]byte, len(info.Cols))
	copy(oldRowHashes, info.Rows)
	copy(oldColHashes, info.Cols)

	// update row/col hashes
	if err = b.updateBlockMatrixInfo(info, blockNum); err != nil {
		return err
	}

	var ok bool
	if ok, err = b.checkValidErase(info, oldRowHashes, oldColHashes); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("invalid erase, more than one row/column was affected")
	}

	return nil
}

func (b *BlockMatrix) checkValidErase(info *BlockMatrixInfo, oldRowHashes [][]byte, oldColHashes [][]byte) (bool, error) {
	var numRowChanged, numColChanged int

	for i := 0; i < info.Size; i++ {
		if !reflect.DeepEqual(oldRowHashes[i], info.Rows[i]) {
			numRowChanged++
		} else if !reflect.DeepEqual(oldColHashes[i], info.Cols[i]) {
			numColChanged++
		}
	}

	return numRowChanged == 1 && numColChanged == 1, nil
}

// Matrix returns a 2D matrix of the blocks in the key value database.
func (b *BlockMatrix) Matrix() ([][]*Block, error) {
	info, err := b.GetBlockMatrixInfo()
	if err != nil {
		return nil, err
	}

	size := b.Size(info.BlockCount)
	// initialize the matrix
	matrix := make([][]*Block, size)
	for i := 0; i < size; i++ {
		matrix[i] = make([]*Block, size)
	}

	// populate the matrix
	for blockNum := 1; blockNum <= (info.Size*info.Size - info.Size); blockNum++ {
		i, j := b.locateBlock(blockNum)
		bytes, err := b.db.Get([]byte(fmt.Sprint(blockNum)), nil)
		if err != nil {
			return nil, err
		}

		block := EmptyBlock()
		if err = json.Unmarshal(bytes, block); err != nil {
			return nil, err
		}

		matrix[i][j] = block
	}

	return matrix, nil
}

// PrintBlockMatrixData prints the data in the block matrix.
func (b *BlockMatrix) PrintBlockMatrixData() error {
	matrix, err := b.Matrix()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)

	info, err := b.GetBlockMatrixInfo()
	if err != nil {
		return err
	}

	for i := 0; i < len(matrix); i++ {
		row := make([]string, 0)
		for j := 0; j < len(matrix); j++ {
			if i == j {
				row = append(row, ".")
			} else {
				row = append(row, fmt.Sprint(matrix[i][j].Data))
			}
		}
		table.Append(row)
	}

	table.Render()

	fmt.Println("size: ", fmt.Sprint(info.Size))
	fmt.Println("count: ", fmt.Sprint(info.BlockCount))
	fmt.Println("rows:")
	for i, s := range info.Rows {
		fmt.Println("\t", i, ": ", s)
	}
	fmt.Println("cols:")
	for i, s := range info.Cols {
		fmt.Println("\t", i, ": ", s)
	}

	return nil
}

// locateBlock returns the row and column of the block with the given block number
func (b *BlockMatrix) locateBlock(blockNum int) (i int, j int) {
	// calculate row index
	if blockNum%2 == 0 {
		s := int(math.Floor(math.Sqrt(float64(blockNum))))
		if blockNum <= s*s+s {
			i = s
		} else {
			i = s + 1
		}
	} else {
		s := int(math.Floor(math.Sqrt(float64(blockNum + 1))))
		col := 0
		if blockNum < s*s+s {
			col = s
		} else {
			col = s + 1
		}

		i = (blockNum - (col*col - col + 1)) / 2
	}

	// calculate column index
	if blockNum%2 == 0 {
		s := int(math.Floor(math.Sqrt(float64(blockNum))))
		row := 0
		if blockNum <= s*s+s {
			row = s
		} else {
			row = s + 1
		}

		j = (blockNum - (row*row - row + 2)) / 2
	} else {
		s := int(math.Floor(math.Sqrt(float64(blockNum + 1))))
		if blockNum < s*s+s {
			j = s
		} else {
			j = s + 1
		}
	}

	return
}

// rowBlockNumbers returns the block numbers for the row at the given index (row index is 0-based)
func (b *BlockMatrix) rowBlockNumbers(rowIndex int, blockCount int) ([]int, error) {
	blocksNums := make([]int, 0)

	// get the blocks under the diagonal
	add := 2
	for col := 0; col < rowIndex; col++ {
		blockNum := rowIndex*rowIndex - rowIndex + add
		blocksNums = append(blocksNums, blockNum)
		add += 2
	}

	// get the blocks above the diagonal
	size := b.Size(blockCount)
	sub := 1
	for col := rowIndex + 1; col < size; col++ {
		blockNum := col*col + col - sub
		blocksNums = append(blocksNums, blockNum)
		sub += 2
	}

	return blocksNums, nil
}

// columnBlockNumbers returns the block numbers for the column at the given index (column index is 0-based)
func (b *BlockMatrix) columnBlockNumbers(colIndex int, blockCount int) ([]int, error) {
	blocksNums := make([]int, 0)

	// get the blocks above the diagonal
	sub := 2*colIndex - 1
	for row := 0; row < colIndex; row++ {
		blockNum := colIndex*colIndex + colIndex - sub
		blocksNums = append(blocksNums, blockNum)
		sub -= 2
	}

	// get the blocks under the diagonal
	size := b.Size(blockCount)
	add := 2*colIndex + 2
	for row := colIndex + 1; row < size; row++ {
		blockNum := row*row - row + add
		blocksNums = append(blocksNums, blockNum)
	}

	return blocksNums, nil
}

func (b *BlockMatrix) GetBlockMatrixInfo() (*BlockMatrixInfo, error) {
	if ok, err := b.db.Has([]byte("info"), nil); err != nil {
		return nil, err
	} else if !ok {
		info := &BlockMatrixInfo{
			Rows: make([][]byte, 1),
			Cols: make([][]byte, 1),
		}

		var bytes []byte
		if bytes, err = json.Marshal(info); err != nil {
			return nil, err
		}

		if err = b.db.Put([]byte("info"), bytes, nil); err != nil {
			return nil, err
		}

		return info, nil
	}

	infoBytes, err := b.db.Get([]byte("info"), nil)
	if err != nil {
		return nil, err
	}

	info := &BlockMatrixInfo{}
	if err = json.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (b *BlockMatrix) calculateRowHash(row int, blockCount int) ([]byte, error) {
	h := sha256.New()
	blocks, err := b.rowBlockNumbers(row, blockCount)
	if err != nil {
		return nil, err
	}

	for _, blockNum := range blocks {
		block, err := b.GetBlockByNumber(blockNum)
		if err != nil {
			return nil, err
		}

		h.Write(block.Hash)
	}

	return h.Sum(nil), nil
}

func (b *BlockMatrix) calculateColumnHash(col int, blockCount int) ([]byte, error) {
	h := sha256.New()
	blocks, err := b.columnBlockNumbers(col, blockCount)
	if err != nil {
		return nil, err
	}

	for _, blockNum := range blocks {
		block, err := b.GetBlockByNumber(blockNum)
		if err != nil {
			return nil, err
		}

		h.Write(block.Hash)
	}

	return h.Sum(nil), nil
}

// updateBlockMatrixSize updates the size of the block matrix and creates empty entries for the new blocks added. This
// prevents any nil pointer references for blocks that haven't been initialized with AddBlock but are still in the matrix.
func (b *BlockMatrix) updateBlockMatrixSize(info *BlockMatrixInfo, newSize int) error {
	numBlocksToAdd := 2 * info.Size
	info.Size = newSize
	for i := info.BlockCount; i < info.BlockCount+numBlocksToAdd; i++ {
		bytes, err := json.Marshal(EmptyBlock())
		if err != nil {
			return err
		}

		if err = b.db.Put([]byte(fmt.Sprint(i)), bytes, nil); err != nil {
			return err
		}
	}

	info.Rows = append(info.Rows, make([]byte, 0))
	info.Cols = append(info.Cols, make([]byte, 0))

	return nil
}

func (b *BlockMatrix) IsValid() (bool, error) {
	info, err := b.GetBlockMatrixInfo()
	if err != nil {
		return false, err
	}

	// check block hashes
	for i := 1; i <= info.BlockCount; i++ {
		var block *Block
		if block, err = b.GetBlockByNumber(i); err != nil {
			return false, err
		}

		if reflect.DeepEqual(block.Hash, block.CalculateHash()) {
			return false, fmt.Errorf("hashes for block %d are not equal", i)
		}
	}

	// check row hashes
	size := b.Size(info.BlockCount)
	for i := 0; i < size; i++ {
		var hash []byte
		if hash, err = b.calculateRowHash(i, info.BlockCount); err != nil {
			return false, err
		}

		if reflect.DeepEqual(info.Rows[i], hash) {
			return false, fmt.Errorf("hashes for row %d are not equal", i)
		}
	}

	// check col hashes
	for i := 0; i < size; i++ {
		var hash []byte
		if hash, err = b.calculateColumnHash(i, info.BlockCount); err != nil {
			return false, err
		}

		if reflect.DeepEqual(info.Cols[i], hash) {
			return false, fmt.Errorf("hashes for column %d are not equal", i)
		}
	}

	// TODO check if there have been invalid deletions

	return true, nil
}
