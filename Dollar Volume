from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import AverageDollarVolume

def initialize(context):
    pipe = Pipeline()
    attach_pipeline(pipe, name = 'my_pipeline')

    #Construuct an average dollar volume factor and add it to the pipeline.
    dollar_volume = AverageDollarVolume(window_length = 30)
    pipe.add(dollar_volume, 'dollar_volume')

    #Define high_dollar_volume filter to be the top 10% of securities by dollar volume.
    high_dollar_volume = dollar_volume.percentile_between(90, 100)

    #Filter to only the top dollar volume securities.
    pipe.set_screen(high_dollar_volume)

    #Set custom benchmark to Tesla
    set_benchmark(symbol('TSLA'))
