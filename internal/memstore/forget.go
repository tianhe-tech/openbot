// Package memstore provides a lightweight persistent memory store for gateway
// conversations, with Ebbinghaus-style forgetting curve support.
package memstore

import (
	"math"
	"time"
)

// λ controls how fast memories decay when unreviewed (per day).
// At λ=0.1, a memory at strength=1.0 reaches ~0.37 after 10 days untouched.
const decayLambda = 0.1

// reviewIntervals are the SM-2–inspired intervals (in days) indexed by recall count.
// After each successful recall the interval grows; once past the table cap it doubles.
var reviewIntervals = []float64{1, 2, 4, 7, 15, 30, 60}

// EffectiveStrength returns the decayed strength given the base strength,
// the original strength at last review, and elapsed days since then.
func EffectiveStrength(strength, daysSince float64) float64 {
	if daysSince <= 0 {
		return strength
	}
	decayed := strength * math.Exp(-decayLambda*daysSince)
	if decayed < 0 {
		return 0
	}
	return decayed
}

// NextReviewAfter returns how long to wait before the next review,
// based on how many times this record has already been recalled.
func NextReviewAfter(recallCnt int) time.Duration {
	idx := recallCnt
	if idx < 0 {
		idx = 0
	}
	var days float64
	if idx < len(reviewIntervals) {
		days = reviewIntervals[idx]
	} else {
		// Beyond table: keep doubling the last interval
		extra := idx - len(reviewIntervals) + 1
		days = reviewIntervals[len(reviewIntervals)-1] * math.Pow(2, float64(extra))
	}
	return time.Duration(days*24) * time.Hour
}

// StrengthenAfterRecall returns an updated strength value after the record is recalled.
// Each recall adds 0.15 and clamps to 1.0.
func StrengthenAfterRecall(current float64) float64 {
	v := current + 0.15
	if v > 1.0 {
		return 1.0
	}
	return v
}
