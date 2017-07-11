from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import CustomFactor, SimpleMovingAverage
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings, BusinessDaysSincePreviousEarnings

def make_pipeline():
    """
    Risk Framework
    """
    ne = BusinessDaysUntilNextEarnings()
    pe = BusinessDaysSincePreviousEarnings()
    # The number of days before/after an announcement that you want to avoid an earnings call for.
    avoid_earnings_days = 1
    does_not_have_earnings = ((ne.isnan() | (ne > avoid_earnings_days)) & (pe > avoid_earnings_days))
    """
    End of Risk Framework
    """
    pipe_screen = (does_not_have_earnings)

    pipe_columns = {
        'next_earnings':ne,
        'prev_earnings':pe
    }

    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)
    return pipe

def before_trading_start(context, data):
    context.results = pipeline_output('tradeable_stocks')
    context.stocks_to_trade = [stock for stock in context.tradeable_stocks
                               if stock in context.results.index]

# Put any initialization logic here.  The context object will be passed to
# the other methods in your algorithm.
def initialize(context):
    set_symbol_lookup_date('2015-01-01')
    context.tradeable_stocks = [
        symbol('AAPL'),
        symbol('AMZN'),
        symbol('GOOG_L'),
        symbol('IBM'),
        symbol('MSFT'),
        symbol('NFLX'),
        symbol('TSLA'),
        symbol('YHOO'),]

    attach_pipeline(make_pipeline(), 'tradeable_stocks')

    # Schedule my rebalance function weekly
    schedule_function(func=earnings_call,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_open(hours=0,minutes=30),
                      half_days=True)

def earnings_call(context, data):
        for stock in context.stocks_to_trade:
            order_target_percent(stock, .5*1.0/len(context.stocks_to_trade))

        for stock in context.portfolio.positions:
            if stock not in context.stocks_to_trade:
                order_target_percent(stock, 0)

# Will be called on every trade event for the securities you specify.
def handle_data(context, data):
    pass
