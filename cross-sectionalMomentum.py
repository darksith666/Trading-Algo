import numpy as np
import pandas as pd

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.data import morningstar as mstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage, Returns, CustomFactor, Latest
from quantopian.pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings, BusinessDaysSincePreviousEarnings


class CrossSectionalMomentum(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, prices):
        prices = pd.DataFrame(prices)
        R = (prices / prices.shift(100))
        out[:] = (R.T - R.T.mean()).T.mean()


def make_pipeline():
    # Basic momentum metrics.
    cross_momentum = CrossSectionalMomentum()
    abs_momentum = Returns(inputs=[USEquityPricing.close], window_length=252)

    # We only want to trade relatively liquid stocks.
    dollar_volume = AverageDollarVolume(window_length=20)
    is_liquid = (dollar_volume > 1e7)
    # We also don't want to trade penny stocks.
    sma_200 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=200)
    not_a_penny_stock = (sma_200 > 5)
    # ROA
    #return_on_assets = morningstar.operation_ratios.roa.latest
    #roa = (return_on_assets > )
    # ROE
    #return_on_equity = mstar.operation_ratios.roe.latest
    #roe = (return_on_equity > 14)
    # P/E ratio
    #price_earnings_ratio = morningstar.valuation_ratios.pe_ratio.latest
    #pe_ratio = (price_earnings_ratio > 0.8)
    # P/S ratio
    #price_sales_ratio = morningstar.valuation_ratios.ps_ratio.latest
    #ps_ratio = (price_sales_ratio < 0.75)
    # PCF ratio
    #price_cashflow_ratio = morningstar.valuation_ratios.pcf_ratio.latest
    #pcf_ratio = (price_cashflow_ratio > 0.8)

    # Before we do any other ranking, we want to throw away the bad assets.
    #optional additions: & roe & pe_ratio & ps_ratio & pcf_ratio
    initial_screen = (is_liquid & not_a_penny_stock)

    # Remove any stocks that failed to meet our initial criteria **before** computing ranks.  This means that the
    combined_rank = (cross_momentum.rank(mask=initial_screen) + abs_momentum.rank(mask=initial_screen))

    # Build Filters representing the top and bottom 5% of stocks by our combined ranking system.
    # We'll use these as our tradeable universe each day.
    longs = combined_rank.percentile_between(95, 100)
    shorts = combined_rank.percentile_between(0, 5)

    # The final output of our pipeline should only include the top/bottom 5% of stocks by our criteria.
    pipe_screen = (longs | shorts)

    pipe_columns = {
        'longs':longs,
        'shorts':shorts,
        'combined_rank':combined_rank,
        'abs_momentum':abs_momentum,
        'cross_momentum':cross_momentum
    }

    # Create pipe
    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)
    return pipe


# The context object will be passed to the other methods in the algorithm.
def initialize(context):
    attach_pipeline(make_pipeline(), 'momentum_metrics')

    context.shorts = None
    context.longs = None
    context.output = None

    # Schedule momentum function weekly.
    schedule_function(momentum, date_rules.month_start())


def before_trading_start(context, data):
    context.output = pipeline_output('momentum_metrics')
    ranks = context.output['combined_rank']

    context.longs = ranks[context.output['longs']]
    context.shorts = ranks[context.output['shorts']]

    context.active_portfolio = context.longs.index.union(context.shorts.index)


def momentum(context, data):
    # Logic for buying long on selected stocks.
    for security in context.longs.index:
        if get_open_orders(security):
            continue
        if data.can_trade(security):
            order_target_percent(security, 0.5 / len(context.longs.index))
    # Logic for shorting stocks.
    for security in context.shorts.index:
        if get_open_orders(security):
            continue
        if data.can_trade(security):
            order_target_percent(security, -0.5 / len(context.shorts.index))
    # Logic for rebalancing portfolio once a stock fails to meet long or short criteria.
    for security in context.portfolio.positions:
        if get_open_orders(security):
            continue
        if data.can_trade(security):
            if security not in (context.longs.index | context.shorts.index):
                order_target_percent(security, 0)


# Will be called on every trade event for the securities specified.
def handle_data(context, data):
    pass
