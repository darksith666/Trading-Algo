from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import AverageDollarVolume

def initialize(context):
    pipe = Pipeline()
    attach_pipeline(pipe, name = 'my_pipeline')

    #Construct an average dollar volume factor and add it to the pipeline.
    peRatio = AverageDollarVolume(window_length = 30)
    pipe.add(dollar_volume, 'dollar_volume')
    psRatio =

    #Define high_dollar_volume filter to be the top 10% of securities by dollar volume.
    high_dollar_volume = dollar_volume.percentile_between(90, 100)

    #Filter to only the top dollar volume securities.
    pipe.set_screen(high_dollar_volume)

from quantopian.pipeline import Pipeline
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import CustomFactor, Returns

def make_pipeline():
#Break this piece of logic out into its own function to make it easier to test and modify in isolation.
    pipe = Pipeline(
        columns = {
            'ROA' : morningstar.operation_ratios.roa.latest,
            'ROE' : morningstar.operation_ratios.roe.latest,
            'PE Ratio' : morningstar.valuation_ratios.pe_ratio.latest,
            'PS Ratio' : morningstar.valuation_ratios.ps_ratio.latest,
            'PCF Ratio' : morningstar.valuation_ratios.pcf_ratio.latest
        })
    ROA = morningstar.operation_ratios.roa.latest
    ROE = morningstar.operation_ratios.roe.latest
    pe_ratio = morningstar.valuation_ratios.pe_ratio.latest
    ps_ratio = morningstar.valuation_ratios.ps_ratio.latest
    pcf_ratio = morningstar.valuation_ratios.pcf_ratio.latest
    return pipe

pipe = make_pipeline()
