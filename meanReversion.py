#Import libraries to be used.
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, Returns

def initialize(context):
    #Define context variables that can be accessed in other methods of algo.
    context.long_leverage = 0.5
    context.short_leverage = -0.5
    context.returns_lookback = 5

    #Rebalance on the first trading of each week at 11AM.
    schedule_function(rebalance,
        date_rules.week_start(days_offset = 0),
        time_rules.market_open(hours = 1, minutes = 30))

    #Create and attach pipeline (dynamic stock selector)
    attach_pipeline(make_pipeline(context), 'mean_reversion')

def make_pipeline(context):
    #Create a pipeline object.

    #Create a dollar_volume factor using default inputs and window_length.
    #This is a builtin factor.
    dollar_volume = AverageDollarVolume(window_length = 1)

    #Define high_dollar_volume filter to be the top 5% of stocks by dollar volume.
    high_dollar_volume = dollar_volume.percentile_between(95, 100)

    #Create a recent_returns factor with a 5-day returns lookback for all securities in high_dollar_volume filter.
    #This is a custome factor defined below (see RecentReturns class).
    recent_returns = Returns(window_length = context.returns_lookback, mask = high_dollar_volume)

    #Define high and low returns filters to be the bottom 10% and top 10$ of securities in the high_dollar_volume group.
    low_returns = recent_returns.percentile_between(0, 10)
    high_returns = recent_returns.percentile_between(90, 100)

    #Define a column dictionary that holds all the factrs.
    pipe_columns = {
        'low_returns':low_returns,
        'high_returns':high_returns,
        'recent_returns':recent_returns,
        'dollar_volume':dollar_volume
    }

    #Add a filter to the pipeline so that only high and low return securities are kept.
    pipe_screen = (low_returns | high_returns)

    #Create a pipeline object with the defined columns and screen.
    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)

    return pipe

def before_trading_start(context, data):
    #Pipeline_output returns a pandas DataFrame with the retulst of the factors and filters.
    context.output = pipeline_output('mean_reversion')

    #Sets the list of securities to long as the securities with a 'True' value in the low_returns column.
    context.long_secs = context.output[context.output['low_returns']]
    #Sets the list of securities to short with a 'True' value in the high_returns column.
    context.short_secs = context.output[context.output['high_returns']]

    #A list of securities that we want to order today.
    context.security_list = context.long_secs.index.union(context.short_secs.index).tolist()
    #alternatively (faster)
    #contex.security_list = set(context.security_list)

def compute_weights(context):
    #Set the allocations to even weights for each long position, and even weights for each short position.
    long_weight = context.long_leverage / len(context.long_secs)
    short_weight = context.short_leverage / len(context.short_secs)

    return long_weight, short_weight

def rebalance(context, data):
    #Rebalancing function called according to schedule_function settings.

    long_weight, short_weight = compute_weights(context)

    #For each security in our universe, order long or short positions according to context.long_secs and context.short_secs
    for stock in context.security_list:
        if data.can_trade(stock):
            if stock in context.long_secs.index:
                order_target_percent(stock, long_weight)
            elif stock in context.short_secs.index:
                order_target_percent(stock, short_weight)

    #Sell all previously held positions not in our new context.security_list.
    for stock in context.portfolio.positions:
        if stock not in context.security_set and data.can_trade(stock):
            order_target_percent(stock, 0)

    #Log the long and short orders each week.
    long.info("This week's longs: "+", ".join([long_.symbol for long_ in context.long_secs.index]))
    long.info("This week's longs: "+", ".join([short_.symbol for short_ in context.short_secs.index]))

def record_vars(context, data):
    #Function called at the end of each day and plots certain variables.

    #Check how many long and short positions in portfolio.
    long = shorts = 0
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            longs += 1
        if position.amount < 0:
            shorts += 1

    #Record and plot the leverage of portfolio over time as well as the number of long and short positions.
    record(leverage = context.account.leverage, long_count = longs, short_count = shorts)
