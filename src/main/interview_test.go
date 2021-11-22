package main

import "testing"

func findNum(arr []int, target int) int {
	tag := tagIndex(arr)
	if target > arr[tag] {
		return -1
	}
	if target < arr[tag + 1] {
		return -1
	}
	if target >= arr[0] {
		return binarySearch(arr[:target + 1], target)
	} else {
		return binarySearch(arr[target + 1:], target)
	}
}

func binarySearch(arr []int, tar int) int {
	l, r := 0, len(arr) - 1
	for l < r {
		mid := (l + r) / 2
		if arr[mid] == tar {
			return mid
		}
		if arr[mid] > tar {
			l = mid
		} else {
			r = mid
		}
	}
	return -1
}

func tagIndex(arr []int) int {
	l, r := 0, len(arr) - 1
	for l < r {
		mid := (l + r) / 2
		if arr[mid] > arr[l] {
			if mid < len(arr) && arr[mid] > arr[mid + 1] {
				return mid
			}
			l = mid
		} else {
			if mid > 0 && arr[mid - 1] > arr[mid] {
				return mid - 1
			}
			r = mid
		}
	}
	return 0
}

func TestTag(t *testing.T) {
	arr := []int{5,6,7,9,0,1,2}
	t.Log(findNum(arr, 9))
}