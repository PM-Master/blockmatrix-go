package blockmatrix

import "crypto/sha256"

type Block struct {
	Data []byte `json:"data"`
	Hash []byte `json:"hash"`
}

func NewBlock(data []byte) *Block {
	hash := calculateHash(data)
	return &Block{
		Data: data,
		Hash: hash,
	}
}

func calculateHash(bytes []byte) []byte {
	h := sha256.New()
	h.Write(bytes)
	return h.Sum(nil)
}

func EmptyBlock() *Block {
	bytes := []byte{0}
	return &Block{
		Data: bytes,
		Hash: calculateHash(bytes),
	}
}

func (b Block) CalculateHash() []byte {
	return calculateHash(b.Data)
}
