package utils

import (
	"strings"
)

// ToLowerCase converts a string to lowercase
func ToLowerCase(s string) string {
	return strings.ToLower(s)
}

// IsValidAggregation checks if the aggregation type is valid
func IsValidAggregation(aggregation string) bool {
	switch strings.ToLower(aggregation) {
	case "avg", "sum", "min", "max", "histogram", "gauge":
		return true
	default:
		return false
	}
}

// GetDefaultInterval returns the default interval for an aggregation type
func GetDefaultInterval(aggregation string) uint32 {
	switch strings.ToLower(aggregation) {
	case "histogram":
		return 10 // 10 seconds
	case "gauge":
		return 30 // 30 seconds
	default:
		return 0
	}
}
