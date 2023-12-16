# Cointegrated-Momentum-Transition
Trading strategy that combines Momentum transition with cointegration analysis for price movement prediction

The following directory provides a trading strategy implementation that focuses on relatively undervalued stocks in a presence of a shift in momentum against their underlying peers. 
That is, the goal of the strategy is in identifying "New Winners" that are potentially undervalued. This implementation is achieved by using two simple metrics.

1) Relative Valuation - achieved by analysing the cointegrated stocks,  grouped by their respective sector benchmark. 
2) Identifying winners shifting and the movement from losers to winners, grouped by their respective sector benchmark.


## Motivation
Absolute valuation is focused on defining a target price, which represents the expected value, estimated by the investor. 
However there is a problem with absolute valuation. First of all, the expectation of value resides on the "very long term", meaning the projection of cash flows,discount rates and other inputs become gradually less accurate, as we increase the time frame. This means that a pegging of a price is based on a set of assumptions that none of us actually now will be fullfilled. Of course, these price targets are marginally changed as new information comes along and its allways useful to have a basis of valuation to understand whether the stock is a buy or a sell. Relative valuation, on the other hand, looks at the current state of peer stocks and attempts to derive their under(over)valuation by comparing them to other peers, meaning the importance of future considerations, becomes less important, since the stock is directly compared with the "present state" of its peers. The usefulness on this approach, is that a price target may not be directly estimated, but we may still infer whether the stock is expensive or cheap.

Peer valuation itself is not enough to guarantee future performance. In fact, when peers diverge, usually there are fundamental reasons for such divergence, which materialize in price discrepancies, and may trigger value traps. An undervalued issue "may seem" that way, but in reality, it represents a idiosyncratic characteristic of the stock, not shared with its peers. On the other hand, we also know that markets over react to reality, and generally, discount prices by too much or by too less (the sole point of Shillers critique). This means that, even if a fundamental trigger affects the underlying price of the stock, that price, may not truly reflect what the stock is ought to value. 

## Universe 
The universe of analysis, focuses on SP500 stocks.

## The strategy
The goal of the strategy is in finding the sweet spot among neglected stocks that may be shifting their tides, by focusing on the Momentum Transition among them, rather than the momentum itself. That is, I'm not interest in stocks, that are already winners, but rather in trying to idenfity the new winners, focusing on undervalued stocks on a relative basis.

### 1. Momentum Transition 
Momentum Transition is captured by first ranking stocks by their momentum accordingly to their respective sector (GICS). After ranking, stocks are further segmented into 5 levels of momentum ranging from [Very Low, Low, Normal, High, Very High], for intervals of 30 days (30D), 90 days (90D) and 1 year (1Y), accordingly to their percentile position. Transition is measured by counting the "UP movements" of each stock from these levels for each time frequency.
For example: If a stock moves from Very Low to Normal in 30D, +2 movement is accounted for Momentum Transition in 30D. 

#### 1.1 Store the Momentum labels on the Database
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/a30ab1f7-9e16-4fb2-9902-e9dcbbee0557)


#### 1.2 Estimate the momentum transition and count up the transitions
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/7b8b559a-1172-4674-afff-071dd7982a9f)
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/d512f0ee-b929-4eb3-b6b7-936ded985c7b)



### 2. Cointegration relative positioning (Valuation)
Relative valuation is captured by analysing the cointegration that stocks have to their sector benchmark, by looking at the residuals of the cointegration regression. This is applicable only to stocks that are cointegrated with their benchmark. To estimate whether stocks are cointegrated, I use a ADF test (Augmented Dickey Fuller) over the residuals to check whether the combined time series have a unit root, indicating whether or not they are stationary over time, and apply a p-value of 5% for rejecting the null hypothesis. 

#### 2.1 ADF Test
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/d3557f2b-4e1c-4b62-95f7-baa6c1ea7342)

#### 2.2. Cointegration Evaluation
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/57313fec-acd5-44ce-895f-e88219e19687)


#### 2.3. Cointegration relative valuation 
There are two approaches in measuring the cointegrated valuation of a stock. The 1st is based on a relative comparison of the percentile position of the residuals among peers and ranking them accordingly again using 5 levels but segmented into ["Very Undervalued", "Undervalued", "Normal", "Overvalued" and "Very Overvalued"].
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/870d8426-aafe-42c8-b3e7-2b09471c46a6)
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/d29b0993-0c90-42ee-b36d-ea9722114124)


#### 2.4 Cointegration absolute valuation 
The second approach standardizes the residuals for each stock and defines thresholds based on the standard deviation of the standardized residuals. Again, 5 levels are created where (Under)/(Over)valued is measured by a distance of 0.5 STD and Very (Under)/(Over)valued by a distance of 1 STD. 
![image](https://github.com/LusoNX/Cointegrated-Momentum-Transition/assets/84282116/da33757c-1486-4803-82e3-74c8d79c84ee)



### 3. Final Implementation 

The final implementation of the strategy relies on combining the two metrics into a decision making strategy. Weightings and positioning is not considered, only a simple signal that looks at positive counts under the Momentum Transition for Undervalued and Very Undervalued stocks.

### 4. Further improvements

#### 4.1 Weighting Scheme
Position weighs can be adjusted accordingly to the strenght of the signal. That is, the larger the number of counts and the more undervalued the issue is, the higher the weight.

#### 4.2 Further clustering 
Seggregation is made at the sector level. The idea, is that stocks within a sector share greater similarities among themselfs. Further granularity can be introduced by using classifications such as GICS Sub-Industry, or extending it using more advanced quantitative methods. One practical example is the usage of KNN clustering, decision trees or even random forest by selecting a set of sector/industry domain features and clustering the stocks accordingly to the identified labels.

#### 4.3 Universe Expansion 
The signal strategy only considers one universe and given the several pre-conditions for the creation of the signal, only a small number of stocks are available after the full screen is applied. This is interest for the generation of trading ideas and potential new issues, but impractical in the context of portfolio creation. As such, increasing the size of the universe may be ideal to create a more diversified strategy implementation.



