package reedsolomon

import (
	"fmt"
	"testing"
)

func TestLRCEncode(t *testing.T) {
	lrc, err := NewLRC(4, 2, 3)
	if err != nil {
		t.Error(err)
	}
	data := make([][]byte, 9)
	testData := []byte("love")
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1)
	}
	for i := 0; i < len(testData); i++ {
		data[i] = []byte{testData[i]}
	}
	err = lrc.Encode(data)
	if err != nil {
		t.Error(err)
	}
}

func TestLocalRepair(t *testing.T) {
	lrc, err := NewLRC(4, 2, 3)
	if err != nil {
		t.Error(err)
	}
	data := make([][]byte, 9)
	testData := []byte("love")
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1)
	}
	for i := 0; i < len(testData); i++ {
		data[i] = []byte{testData[i]}
	}
	err = lrc.Encode(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("data:", data)
	data[0] = data[0][0:0]
	data[3] = data[3][0:0]
	fmt.Println("error data:", data)
	lrc.LocalRepair(data)
	fmt.Println("after local repair, data:", data)
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	err = lrc.LocalRepair(data)
	if err != nil {
		fmt.Println("expect error: ", err)
	}
}

func TestGlobalRepair(t *testing.T) {
	lrc, err := NewLRC(4, 2, 3)
	if err != nil {
		t.Error(err)
	}
	data := make([][]byte, 9)
	testData := []byte("love")
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1)
	}
	for i := 0; i < len(testData); i++ {
		data[i] = []byte{testData[i]}
	}
	err = lrc.Encode(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("data:", data, &data[0][0], &data[1][0])
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	fmt.Println("error data:", data)
	err = lrc.GlobalRepair(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("after local repair, data:", data, &data[0][0], &data[1][0])
	// repair 4 broken nodes, any 4 broken nodes can be repaired
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	data[2] = data[2][0:0]
	data[3] = data[3][0:0]
	fmt.Println("error data:", data)
	err = lrc.GlobalRepair(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("after local repair, data:", data)
	// repair 5 broken nodes
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	data[2] = data[2][0:0]
	data[4] = data[4][0:0]
	data[5] = data[5][0:0]
	fmt.Println("error data:", data)
	err = lrc.GlobalRepair(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("after local repair, data:", data)
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	data[5] = data[5][0:0]
	data[6] = data[6][0:0]
	data[7] = data[7][0:0]
	fmt.Println("error data:", data)
	err = lrc.GlobalRepair(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("after local repair, data:", data)
	data[0] = data[0][0:0]
	data[1] = data[1][0:0]
	data[2] = data[2][0:0]
	data[4] = data[4][0:0]
	fmt.Println("error data:", data)
	err = lrc.GlobalRepair(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("after local repair, data:", data)
	nums := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	m := 4
	n := len(nums)
	indexs := zuheResult(n, m)
	cnt := 0
	err = findNumsByIndexs(nums, indexs, func(seqs []int) error {
		for _, idx := range seqs {
			data[idx] = data[idx][0:0]
		}
		fmt.Println("error data:", data)
		err = lrc.GlobalRepair(data)
		if err != nil {
			return err
		}
		fmt.Println("after local repair, data:", data)
		cnt++
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("finish success count", cnt)
}

func zuheResult(n int, m int) [][]int {
	if m < 1 || m > n {
		fmt.Println("Illegal argument. Param m must between 1 and len(nums).")
		return [][]int{}
	}

	result := make([][]int, 0, mathZuhe(n, m))
	indexs := make([]int, n)
	for i := 0; i < n; i++ {
		if i < m {
			indexs[i] = 1
		} else {
			indexs[i] = 0
		}
	}
	result = addTo(result, indexs)
	for {
		find := false
		for i := 0; i < n-1; i++ {
			if indexs[i] == 1 && indexs[i+1] == 0 {
				find = true

				indexs[i], indexs[i+1] = 0, 1
				if i > 1 {
					moveOneToLeft(indexs[:i])
				}
				result = addTo(result, indexs)

				break
			}
		}
		if !find {
			break
		}
	}

	return result
}

func addTo(arr [][]int, ele []int) [][]int {
	newEle := make([]int, len(ele))
	copy(newEle, ele)
	arr = append(arr, newEle)

	return arr
}

func moveOneToLeft(leftNums []int) {
	sum := 0
	for i := 0; i < len(leftNums); i++ {
		if leftNums[i] == 1 {
			sum++
		}
	}

	for i := 0; i < len(leftNums); i++ {
		if i < sum {
			leftNums[i] = 1
		} else {
			leftNums[i] = 0
		}
	}
}

func findNumsByIndexs(nums []int, indexs [][]int, f func([]int) error) error {
	if len(indexs) == 0 {
		return nil
	}
	for _, v := range indexs {
		line := make([]int, 0)
		for j, v2 := range v {
			if v2 == 1 {
				line = append(line, nums[j])
			}
		}
		if err := f(line); err != nil {
			return err
		}
	}
	return nil
}

func mathZuhe(n int, m int) int {
	return jieCheng(n) / (jieCheng(n-m) * jieCheng(m))
}

func jieCheng(n int) int {
	result := 1
	for i := 2; i <= n; i++ {
		result *= i
	}

	return result
}

func TestLRCVerity(t *testing.T) {
	lrc, err := NewLRC(4, 2, 3)
	if err != nil {
		t.Error(err)
	}
	data := make([][]byte, 9)
	testData := []byte("love")
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1)
	}
	for i := 0; i < len(testData); i++ {
		data[i] = []byte{testData[i]}
	}
	err = lrc.Encode(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("data:", data)
	ret, err := lrc.Verify(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("shards verity result %v, finally data %v\n", ret, data)
	data[0][0] = 'a'
	ret, err = lrc.Verify(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("shards verity result %v, finally data %v\n", ret, data)
	data[0][0] = 'l'
	ret, err = lrc.Verify(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("shards verity result %v, finally data %v\n", ret, data)
	five := data[5][0]
	data[5][0] = 'a'
	ret, err = lrc.Verify(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("shards verity result %v, finally data %v\n", ret, data)
	data[5][0] = five
	ret, err = lrc.Verify(data)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("shards verity result %v, finally data %v\n", ret, data)
}

func TestLRCGeneratePolicy(t *testing.T) {
	lrc, err := NewLRC(4, 2, 3)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err := lrc.GeneratePolicy(nil, []int{0})
	fmt.Println("[case 1] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{4})
	fmt.Println("[case 2] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{6})
	fmt.Println("[case 3] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{5})
	fmt.Println("[case 4] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{3})
	fmt.Println("[case 5] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{0, 1, 4})
	fmt.Println("[case 6] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{2, 3, 5})
	fmt.Println("[case 7] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{0, 1, 7}, []int{2, 3, 5, 6})
	fmt.Println("[case 8] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{2, 3, 7}, []int{0, 1, 4, 6, 8})
	fmt.Println("[case 9] nextLoadShards", nextLoadShards, "result: error", err)
	nextLoadShards, err = lrc.GeneratePolicy([]int{6, 7}, []int{0, 1, 2, 4})
	fmt.Println("[case 10] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{1, 2})
	fmt.Println("[case 11] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{3, 5, 6, 7, 8})
	fmt.Println("[case 12] nextLoadShards", nextLoadShards, "result: error", err)
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{2, 5, 6, 7, 8})
	fmt.Println("[case 13] nextLoadShards", nextLoadShards, "result: error", err)
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{2, 5, 6, 7})
	fmt.Println("[case 14] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{}, []int{2, 6, 7, 8})
	fmt.Println("[case 15] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{2, 3, 4}, []int{0, 1, 7, 8})
	fmt.Println("[case 16] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{2, 3, 4}, []int{0, 1, 6, 7, 8})
	fmt.Println("[case 17] nextLoadShards", nextLoadShards, "result: error", err)
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{8})
	fmt.Println("[case 18] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{0, 8})
	fmt.Println("[case 19] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{0, 1, 8})
	fmt.Println("[case 20] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{0, 1, 2, 8})
	fmt.Println("[case 21] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{0, 1, 2, 5, 8})
	fmt.Println("[case 22] nextLoadShards", nextLoadShards, "result: error", err)
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 5, 6}, []int{0, 1, 2, 4, 7})
	fmt.Println("[case 23] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{5, 6}, []int{0, 1, 2, 4, 7})
	fmt.Println("[case 24] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{0, 2}, []int{1, 3})
	fmt.Println("[case 25] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{0, 2}, []int{1, 3, 5})
	fmt.Println("[case 26] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{0, 2}, []int{1, 3, 5, 6})
	fmt.Println("[case 27] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{0, 2}, []int{1, 3, 5, 6, 7})
	fmt.Println("[case 28] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{3, 6, 7}, []int{0, 1, 2, 4, 5})
	fmt.Println("[case 29] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	lrc, err = NewLRC(28, 2, 3)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{1})
	fmt.Println("[case 30] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{14})
	fmt.Println("[case 31] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy(nil, []int{1, 15})
	fmt.Println("[case 32] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
	nextLoadShards, err = lrc.GeneratePolicy([]int{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29}, []int{1, 15})
	fmt.Println("[case 33] nextLoadShards", nextLoadShards, "result: error", err)
	if err != nil {
		t.Error(err)
	}
}
