import numpy as np
import pandas as pd

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.data import morningstar as mstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, Returns, CustomFactor
from quantopian.pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings
from quantopian.pipeline.filters.morningstar import Q500US

def make_pipeline(context):
    adv = AverageDollarVolume(
        window_length=30,
        mask=USEquityPricing.volume.latest > 0,
    )

    quantile_returns = Returns(window_length = context.returns_lookback, mask = adv.notnan()).quantiles(context.returns_quantiles)

    pipe_screen = ((BusinessDaysUntilNextEarnings().eq(1)) & Q500US() & adv.percentile_between(95, 100))

    pipe_columns = {
        'quantile_returns':quantile_returns
    }

    pipe = Pipeline(columns = pipe_columns, screen = pipe_screen)
    return pipe

def initialize(context):
    context.returns_lookback = 5
    context.returns_quantiles = 5
    context.days_to_hold = 1
    context.max_days_to_hold = 6
    context.max_in_one = 1

    context.longs = [[]] * context.days_to_hold
    context.shorts = [[]] * context.days_to_hold

    attach_pipeline(make_pipeline(context), 'my_pipeline')

    schedule_function(reversal, date_rules.every_day(), time_rules.market_open())

def before_trading_start(context, data):
    context.output = pipeline_output('my_pipeline')

    def update_record(record, new_item):
        record.insert(0, new_item)
        if len(record) > context.max_days_to_hold:
            del(record[-1])
        while len(record) > context.days_to_hold and len(record[-1]) == 0:
            del(record[-1])
        if sum(map(lambda l: 0 if len(l) == 0 else 1, record)) > context.days_to_hold:
            del(record[-1])

    update_record(context.longs, context.output.index[
        context.output['quantile_returns'] == 0
    ])
    update_record(context.shorts, context.output.index[
        context.output['quantile_returns'] ==
            context.returns_quantiles - 1
    ])

def reversal(context, data):
    long_list = [equity for sublist in context.longs for equity in sublist]
    short_list = [equity for sublist in context.shorts for equity in sublist]

    for equity in long_list:
        if data.can_trade(equity):
            order_target_percent(equity, min(0.5 / len(long_list), context.max_in_one))
    for equity in short_list:
        if data.can_trade(equity):
            order_target_percent(equity, -min(0.5 / len(short_list), context.max_in_one))

    for position in context.portfolio.positions:
        if position not in long_list + short_list:
            order_target_percent(position, 0)

def handle_data(context, data):
    pass
