import numpy as np
import pandas as pd

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.data import morningstar as mstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage, Returns, CustomFactor, Latest
# Key import values for this algorithm.
# Quantopian also offers a premium dataset which includes up-to-date values for these factors.
# Free dateset time limits = 01/01/2007 to 22/12/2014.
from quantopian.pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings, BusinessDaysSincePreviousEarnings

# Metadata
__author__ = 'Oliver Guy'
__email__ = 'oliver.guy.16@ucl.ac.uk'
__prevxp__ = 0
__longdescr__ = 'A two-part strategy for trading volatility during earnings announcements'

class CrossSectionalMomentum(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, prices):
        prices = pd.DataFrame(prices)
        R = (prices / prices.shift(100))
        out[:] = (R.T - R.T.mean()).T.mean()

def make_pipeline(context):
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
    # ROE
    #return_on_equity = mstar.operation_ratios.roe.latest
    #roe = (return_on_equity > 14)
    # P/E ratio
    #price_earnings_ratio = mstar.valuation_ratios.pe_ratio.latest
    #pe_ratio = (price_earnings_ratio > 0.8)
    # P/S ratio
    #price_sales_ratio = mstar.valuation_ratios.ps_ratio.latest
    #ps_ratio = (price_sales_ratio < 0.75)
    # PCF ratio
    #price_cashflow_ratio = mstar.valuation_ratios.pcf_ratio.latest
    #pcf_ratio = (price_cashflow_ratio > 0.8)
    # before doing any other ranking, we want to discard the bad assets.
    initial_screen = (
        is_liquid & non_penny_stock
        #& roe & pe_ratio
        #& ps_ratio & pcf_ratio)

    # Rank (combined) stocks for momentum trading.
    combined_rank = (cross_momentum.rank(mask = initial_screen) + abs_momentum.rank(mask = initial_screen))

    # Construct filters representing the top/bottom 5% of stocks by the combined ranking system.
    # This forms the trading universe each week.
    longs = combined_rank.percentile_between(95, 100)
    shorts = combined_rank.percentile_between(0, 5)
    """
    End of Momentum
    """

    """
    Start of risk framework for avoiding earnings announcement for Momentum
    """
    # Risk parameters for avoiding volatility immediately around earnings announcements.
    ne = BusinessDaysUntilNextEarnings()
    pe = BusinessDaysSincePreviousEarnings()
    # No. of days before/after an announcement to be avoided.
    avoid_earnings_days = 2
    # It is common that the value for BusinessDaysUntilNextEarnings() is NaaN because we don't know the precise date of an earnings announcement until about 2 weeks before.
    does_not_have_earnings = ((ne.isnan() | (ne > avoid_earnings_days)) & (pe > avoid_earnings_days))
    """
    End of risk framework
    """

    """
    Start of Momentum Reverse
    """
    adv = AverageDollarVolume(
        window_length=30,
        mask=USEquityPricing.volume.latest > 0,
    )
    quantile_returns = Returns(window_length = context.returns_lookback, mask = adv.notnan()).quantiles(context.returns_quantiles)
    """
    End of Momentum Reverse
    """

    # Establishing final pipe screen and columns to passed on.
    pipe_screen = (initial_screen & (BusinessDaysUntilNextEarnings().eq(1)) & adv.percentile_between(95, 100))

    pipe_columns = {
        'longs':longs,
        'shorts':shorts,
        'combined_rank':combined_rank,
        'abs_momentum':abs_momentum,
        'cross_momentum':cross_momentum,
        'does_not_have_earnings':does_not_have_earnings,
        'next_earnings':ne,
        'prev_earnings':pe,
        'quantile_returns':quantile_returns
    }

    # Create pipe.
    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)
    return pipe

def before_trading_start(context, data):
    context.output = pipeline_output('momentum_metrics')
    ranks = context.output['combined_rank']

    context.longs = ranks[context.output['longs']]
    context.shorts = ranks[context.output['shorts']]

    context.active_portfolio = context.longs.index.union(context.shorts.index)
    update_universe(context.active_portfolio)

    context.results = pipeline_output('momentum_metrics')
    context.stocks_to_trade = context.output.index

    def update_record(record, new_item):
        record.insert(0, new_item)
        if len(record) > context.max_days_to_hold:
            del(record[-1])
        while len(record) > context.days_to_hold and len(record[-1]) == 0:
            del(record[-1])
        if sum(map(lambda l: 0 if len(l) == 0 else 1, record)) > context.days_to_hold:
            del(record[-1])

    update_record(context.longstock, context.output.index[
        context.output['quantile_returns'] == 0
    ])
    update_record(context.shortstock, context.output.index[
        context.output['quantile_returns'] ==
            context.returns_quantiles - 1
    ])

# The context object will be passed to the other methods in the algorithm.
def initialize(context):
    """
    Trading model parameters. These can be altered depending on broker relevant information.
    """
    set_slippage(slippage.VolumeShareSlippage(volume_limit = 0.025, price_impact = 0.1))
    set_commission(commission.PerShare(cost = 0.0075, min_trade_cost = 1))
    # Benchmark set to S&P500 (symbol = SPY)
    set_benchmark(sid(8554))
    """
    End of Trading model parameters
    """

    context.returns_lookback = 5
    context.returns_quantiles = 5
    context.days_to_hold = 1
    context.max_days_to_hold = 6
    context.max_in_one = 1

    attach_pipeline(make_pipeline(context), 'momentum_metrics')

    context.shorts = None
    context.longs = None
    context.output = None

    context.longstock = [[]] * context.days_to_hold
    context.shortstock = [[]] * context.days_to_hold

    schedule_function(func = reversal, date_rule = date_rules.every_day(), time_rule = time_rules.market_open())
    # Schedule momentum function weekly. Open orders are cancelled daily.
    schedule_function(func = momentum, date_rule = date_rules.week_start(), time_rule = time_rules.market_open())
    schedule_function(func = cancel_open_orders, date_rule = date_rules.every_day(), time_rule = time_rules.market_close(), half_days = True)
    # Schedule earnings function to run daily, so as to check for pertinent earnings announcement dates.
    schedule_function(func=earnings_call, date_rule=date_rules.every_day(), time_rule=time_rules.market_open(hours=0,minutes=30), half_days=True)

# Called daily to ensure the momentum strategy sells stocks which are no longer performing to the momentum strategy's parameters.
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

def earnings_call(context, data):
    # Will cease momentum trading a stock if the date falls within 2 days of a company's earning call.
    for stock in context.portfolio.positions:
        if stock not in context.stocks_to_trade:
            order_target_percent(stock, 0)

def reversal(context, data):
    long_list = [equity for sublist in context.longstock for equity in sublist]
    short_list = [equity for sublist in context.shortstock for equity in sublist]

    for equity in long_list:
        if data.can_trade(equity):
            order_target_percent(equity, min(0.5 / len(long_list), context.max_in_one))
    for equity in short_list:
        if data.can_trade(equity):
            order_target_percent(equity, -min(0.5 / len(short_list), context.max_in_one))

    for position in context.portfolio.positions:
        if position not in long_list + short_list:
            order_target_percent(position, 0)

# Will be called on every trade event for the securities specified to record the leverage.
def handle_data(context, data):
    record(positions=len(context.stocks_to_trade),
           leverage=context.account.leverage)
