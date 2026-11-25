package engine

import "math"

// FeeSchedule is the commission model (docs/spec FR-40).
type FeeSchedule struct {
	PerShare               float64 // commission per share
	MinimumPerOrder        float64
	MaximumFractionOfValue float64 // cap as a fraction of trade value, 0 for none
	AddRebatePerShare      float64 // credit per share for fills that added liquidity
}

// IBKRFixed is the Interactive Brokers Pro Fixed schedule: $0.005 per share, $1.00 minimum, 1% of value maximum.
var IBKRFixed = FeeSchedule{PerShare: 0.005, MinimumPerOrder: 1.0, MaximumFractionOfValue: 0.01}

// cumulative commission of an order after cumulativeShares traded for cumulativeValue dollars.
func (fees FeeSchedule) cumulative(cumulativeShares int64, cumulativeValue float64, addedShares int64) float64 {
	if cumulativeShares == 0 {
		return 0
	}
	commission := math.Max(fees.PerShare*float64(cumulativeShares), fees.MinimumPerOrder)
	if fees.MaximumFractionOfValue > 0 {
		commission = math.Min(commission, fees.MaximumFractionOfValue*cumulativeValue)
	}
	return commission - fees.AddRebatePerShare*float64(addedShares)
}
