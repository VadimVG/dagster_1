from dagster import op


@op
def test(context)-> int:
    context.log.info('test')
    return 1