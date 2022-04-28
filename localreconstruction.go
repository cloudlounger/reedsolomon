package reedsolomon

import (
	"errors"
)

type LRCEncoder interface {
	Encode(shards [][]byte) error
	LocalRepair(shards [][]byte) error
	GlobalRepair(shards [][]byte) error
	Verify(shards [][]byte) (bool, error)
}

type LRC struct {
	dataShards   int
	localShards  int
	globalShards int
	totalShards  int
	localLeft    Encoder
	localRight   Encoder
	global       Encoder
	options      options
}

var ErrLocalShards = errors.New("error local shards, only support 2 local shards in current version")
var ErrDataShards = errors.New("error data shards, the number should be great than 0 and be the power of 2")

func NewLRC(dataShards, localShards int, globalShards int, opts ...Option) (encoder LRCEncoder, err error) {
	if localShards != 2 {
		err = ErrLocalShards
		return
	}
	if dataShards <= 0 || dataShards%2 == 1 {
		err = ErrDataShards
		return
	}
	var (
		localLeft        Encoder
		localRight       Encoder
		global           Encoder
		localLeftMatrix  matrix
		localRightMatrix matrix
		globalMatrix     matrix
		localSize        int
		_globalShards    int
		_localShards     int
		options          options
	)
	localSize = dataShards / 2
	_globalShards = globalShards + 1
	_localShards = localSize + 1
	globalMatrix, err = buildMatrix(dataShards, _globalShards+dataShards)
	if err != nil {
		return
	}
	localLeftMatrix, err = buildMatrix(localSize, _localShards)
	if err != nil {
		return
	}
	localRightMatrix, err = buildMatrix(localSize, _localShards)
	if err != nil {
		return
	}
	for col := 0; col < localSize; col++ {
		localLeftMatrix[localSize][col] = globalMatrix[dataShards][col]
		localRightMatrix[localSize][col] = globalMatrix[dataShards][col+localSize]
	}
	localLeft, options, err = newReedSolomonWithMatrix(localSize, 1, localLeftMatrix, opts...)
	if err != nil {
		return
	}
	localRight, options, err = newReedSolomonWithMatrix(localSize, 1, localRightMatrix, opts...)
	if err != nil {
		return
	}
	global, options, err = newReedSolomonWithMatrix(dataShards, _globalShards, globalMatrix, opts...)
	if err != nil {
		return
	}

	return &LRC{
		dataShards:   dataShards,
		localShards:  2,
		globalShards: globalShards,
		totalShards:  dataShards + localShards + globalShards,
		localLeft:    localLeft,
		localRight:   localRight,
		global:       global,
		options:      options,
	}, nil
}

func (l *LRC) Encode(shards [][]byte) error {
	if len(shards) != l.totalShards {
		return ErrTooFewShards
	}
	if err := checkShards(shards, false); err != nil {
		return err
	}
	globalData := make([][]byte, l.totalShards-1)
	for i := 0; i < l.dataShards; i++ {
		globalData[i] = shards[i]
	}
	for i := l.dataShards + 1; i < l.totalShards; i++ {
		globalData[i-1] = shards[i]
	}
	if err := l.global.Encode(globalData); err != nil {
		return err
	}
	localSize := l.dataShards / 2
	localLeftData := make([][]byte, localSize+1)
	for i := 0; i < localSize; i++ {
		localLeftData[i] = shards[i]
	}
	localLeftData[localSize] = shards[l.dataShards]
	if err := l.localLeft.Encode(localLeftData); err != nil {
		return err
	}
	galMulSliceXor(1, shards[l.dataShards], shards[l.dataShards+1], &l.options)
	return nil
}

// LocalRepair 局部修复
// shards包含datashard和parityshard，局部修复需要将局部数据填充到正确的位置。
func (l *LRC) LocalRepair(shards [][]byte) error {
	if len(shards) != l.dataShards+l.localShards+l.globalShards {
		return ErrTooFewShards
	}
	var (
		leftCnt         int
		rightCnt        int
		needRepairLeft  bool
		needRepairRight bool
	)
	needRepairLeft = true
	needRepairRight = true
	localSize := l.dataShards / 2
	for i := 0; i < l.dataShards+2; i++ {
		if len(shards[i]) != 0 {
			if i < localSize || i == l.dataShards {
				leftCnt++
			} else {
				rightCnt++
			}
		}
	}
	if leftCnt == 0 {
		needRepairLeft = false
	}
	if rightCnt == 0 {
		needRepairRight = false
	}

	// 最大尝试修复
	if needRepairLeft && leftCnt == localSize {
		localLeftData := make([][]byte, localSize+1)
		for i := 0; i < localSize; i++ {
			localLeftData[i] = shards[i]
		}
		localLeftData[localSize] = shards[l.dataShards]
		if err := l.localLeft.Reconstruct(localLeftData); err != nil {
			return err
		}
		for i := 0; i < localSize; i++ {
			if len(shards[i]) == 0 {
				shards[i] = localLeftData[i]
			}
		}
		if len(shards[l.dataShards]) == 0 {
			shards[l.dataShards] = localLeftData[localSize]
		}
	}
	// 最大尝试修复
	if needRepairRight && rightCnt == localSize {
		localRightData := make([][]byte, localSize+1)
		for i := 0; i < localSize; i++ {
			localRightData[i] = shards[i+localSize]
		}
		localRightData[localSize] = shards[l.dataShards+1]
		if err := l.localRight.Reconstruct(localRightData); err != nil {
			return err
		}
		for i := 0; i < localSize; i++ {
			if len(shards[i+localSize]) == 0 {
				shards[i+localSize] = localRightData[i]
			}
		}
		if len(shards[l.dataShards+1]) == 0 {
			shards[l.dataShards+1] = localRightData[localSize]
		}
	}
	if (needRepairLeft && leftCnt < localSize) ||
		(needRepairRight && rightCnt < localSize) {
		return ErrTooFewShards
	}
	return nil
}

func checkAllRepaired(shards [][]byte) bool {
	for i := 0; i < len(shards); i++ {
		if len(shards[i]) == 0 {
			return false
		}
	}
	return true
}

func (l *LRC) GlobalRepair(shards [][]byte) error {
	if len(shards) != l.dataShards+l.localShards+l.globalShards {
		return ErrTooFewShards
	}
	// 先尝试local修复
	l.LocalRepair(shards)
	if checkAllRepaired(shards) {
		return nil
	}
	if len(shards[l.dataShards]) != 0 && len(shards[l.dataShards+1]) != 0 {
		galMulSliceXor(1, shards[l.dataShards], shards[l.dataShards+1], &l.options)
	} else {
		shards[l.dataShards+1] = shards[l.dataShards+1][0:0]
	}
	// global修复
	globalData := make([][]byte, l.dataShards+l.globalShards+1)
	for i := 0; i < l.dataShards; i++ {
		globalData[i] = shards[i]
	}
	for i := 0; i < l.globalShards+1; i++ {
		globalData[l.dataShards+i] = shards[l.dataShards+1+i]
	}
	if err := l.global.Reconstruct(globalData); err != nil {
		return err
	}
	for i := 0; i < l.dataShards; i++ {
		if len(shards[i]) == 0 {
			shards[i] = globalData[i]
		}
	}
	for i := 0; i < l.globalShards+1; i++ {
		if len(shards[l.dataShards+1+i]) == 0 {
			shards[l.dataShards+1+i] = globalData[l.dataShards+i]
		}
	}
	// 尝试local修复
	shards[l.dataShards+1] = shards[l.dataShards+1][0:0] // 将第二个localParity标坏
	return l.LocalRepair(shards)
}

func (l *LRC) Verify(shards [][]byte) (ret bool, err error) {
	if len(shards) != l.dataShards+l.localShards+l.globalShards {
		err = ErrTooFewShards
		return
	}
	localSize := l.dataShards / 2
	localLeftData := make([][]byte, localSize+1)
	for i := 0; i < localSize; i++ {
		localLeftData[i] = shards[i]
	}
	localLeftData[localSize] = shards[l.dataShards]
	ret, err = l.localLeft.Verify(localLeftData)
	if err != nil || !ret {
		return
	}
	localRightData := make([][]byte, localSize+1)
	for i := 0; i < localSize; i++ {
		localRightData[i] = shards[localSize+i]
	}
	localRightData[localSize] = shards[l.dataShards+1]
	ret, err = l.localRight.Verify(localRightData)
	if err != nil || !ret {
		return
	}
	globalData := make([][]byte, l.dataShards+l.globalShards+1)
	for i := 0; i < l.dataShards; i++ {
		globalData[i] = shards[i]
	}
	for i := 0; i < l.globalShards+1; i++ {
		globalData[l.dataShards+i] = shards[l.dataShards+i+1]
	}
	sencondParity := shards[l.dataShards+1]
	globalData[l.dataShards] = make([]byte, len(shards[l.dataShards+1]))
	copy(globalData[l.dataShards], shards[l.dataShards+1])
	galMulSliceXor(1, shards[l.dataShards], globalData[l.dataShards], &l.options)
	ret, err = l.global.Verify(globalData)
	shards[l.dataShards+1] = sencondParity
	return
}
