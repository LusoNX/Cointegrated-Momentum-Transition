#Updates the entire data set, from the prices until all metrics
import sys
sys.path.append(r"DATABASE")
sys.path.append(r"\StatsTools")

from DATABASE import getData

from StatsTools import CorrelationStats
from StatsTools import MomentumStats
from StatsTools import UniverseFilter
def main():
    getData.update_price_data()
    CorrelationStats.correlation_stats_update()
    MomentumStats.momentum_update()
    UniverseFilter.universe_update()

main()