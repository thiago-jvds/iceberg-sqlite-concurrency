
from test_caching_tables import *

def main(log=True, graph=True):
    one_cache_one_filter_repeat_10(graph, log)
    one_cache_all_filters(graph, log)
    one_cache_one_limit_repeat_10(graph, log)


if __name__ == "__main__":
    main()
