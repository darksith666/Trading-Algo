import numpy as np
import pandas as pd

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.data import morningstar as mstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, Returns, CustomFactor
from quantopian.pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings, BusinessDaysSincePreviousEarnings

class CrossSectionalMomentum(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, prices):
        prices = pd.DataFrame(prices)
        R = (prices / prices.shift(100))
        out[:] = (R.T - R.T.mean()).T.mean()

def make_pipeline():
"""
Start of Momentum pipe contents
"""
    # Basic momentum metrics
    cross_momentum = CrossSectionalMomentum()
    abs_momentum = Returns(inputs = [USEquityPricing.close], window_length = 252)

    # We only want to trade relatively liquid stocks.
    # Defined as any stock that has $10,000,000 average daily dollar volume over the last 20 days (1 month).
    dollar_volume = AverageDollarVolume(window_length = 20)
    is_liquid = (dollar_volume > 1e7)
    # We also don't wamt to trade penny stocks.
    # Defined as any stock with an average price of less than $5 over the last 252 days (1 year).
    sma_252 = SimpleMovingAverage(inputs = [USEquityPricing.close], window_length = 252)
    non_penny_stock = (sma_252 > 5)

    # We want to discard the bad assets.
    initial_screen = (is_liquid & non_penny_stock)

    # Rank (combined) stocks for momentum trading.
    combined_rank = (cross_momentum.rank(mask = initial_screen) + abs_momentum.rank(mask = initial_screen))

    # Construct filters representing the top/bottom 5% of stocks by the combined ranking system.
    # This forms the trading universe each day.
    longs = combined_rank.percentile_between(95, 100)
    shorts = combined_rank.percentile_between(0, 5)
"""
End of Momentum
"""

"""
Start of risk framework for avoiding earnings announcement for Momentum
"""
    # Risk framework for avoiding earnings announcement
    ne = BusinessDaysUntilNextEarnings()
    pe = BusinessDaysSincePreviousEarnings()
    pipe.add(ne, 'next_earnings')
    pipe.add(pe, 'prev_earnings')
    # No. of days before/after an announcement to be avoided.
    avoid_earnings_days = 2
    does_not_have_earnings = ((ne.isnan() | (ne > avoid_earnings_days)) & (pe > avoid_earnings_days))
"""
End of risk framework
"""

"""
Start of Momentum Reverse
"""
# To be added later.
"""
End of Momentum Reverse
"""

    # Establishing pipe contents.
    pipe_screen = (initial_screen)

    pipe_columns = {
        'longs':longs,
        'shorts':shorts,
        'combined_rank':combined_rank,
        'abs_momentum':abs_momentum,
        'cross_momentum':cross_momentum,
        'does_not_have_earnings':does_not_have_earnings
    }

    # Create pipe.
    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)
    return pipe

def initialize(context):
"""
Trading model parameters
"""
    set_slippage(slippage.VolumeShareSlippage(volume_limit = 0.025, price_impact = 0.1))
    set_commission(commission.PerShare(cost = 0.0075, min_trade_cost = 1))
    # Benchmark set to S&P500 (symbol = SPY)
    set_benchmark(sid(8554))
"""
End of Trading model parameters
"""

    attach_pipeline(make_pipeline(), 'stock_selection')

    context.shorts = None
    context.longs = None
    context.output = None

    # Schedule rebalance function weekly.
    schedule_function(func = momentum, date_rule = date_rules.week_start(), time_rule = time_rules.market_open(minutes = 30), half_days = True)
    schedule_function(func = cancel_open_orders, date_rule = date_rules.every_day(), time_rule = time_rules.market_close())

def before_trading_start(context, data):
    context.output = pipeline_output('stock_selection')
    ranks = context.ouput['combined_rank']

    context.longs = ranks[context.output['longs']]
    context.shorts = ranks[context.output['shorts']]

    context.longs.index.union(context.shorts.index)
    update_universe(context.active.portfolio)

    context.results = pipeline_output('')
    context.stocks_to_trade = [stock for stock in context.stock_selection if stock in context.results.index]

# Will be called on every trade event for the securities specified.
def handle_data(context, data):
    record(lever = context.account.leverage, exposure = context.account.net_leverage, num_pos = len(context.portfolio.positions))

def cancel_open_orders(context, data):
    open_orders = get_open_orders()
    for security in open_orders:
        for order in open_orders[security]:
            cancel_order(order)

def momentum(context, data):
    # Logic for buying long on selected stocks.
    for security in context.longs.index:
        if get_open_orders(security):
            continue
        if security in data:
            order_target_percent(security, 0.5 / len(context.longs.index))

    # Logic for shorting stocks.
    for security in context.shorts.index:
        if get_open_orders(security):
            continue
        if security in data:
            order_target_percent(security, -0.5 / len(context.shorts.index))

    # Logic for rebalancing portfolio once a stock fails to meet long or short criteria.
    for security in context.portfolio.positions:
        if get_open_orders(security):
            continue
        if security in data:
            if security not in (context.longs.index | context.shorts.index):
                order_target_percent(security, 0)
