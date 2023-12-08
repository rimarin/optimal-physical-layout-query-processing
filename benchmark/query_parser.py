import sqlparse
from sqlparse.sql import Where, Identifier, Comparison


class QueryParser:

    def __init__(self, query_string: str = None):
        self.query_string = query_string

    def extract_columns_from_where(self, query_string: str = None) -> list:
        # Parse the SQL query
        if query_string:
            sql_query = query_string
        else:
            sql_query = self.query_string

        parsed = sqlparse.parse(sql_query)

        where_columns = []
        for stmt in parsed:
            for token in stmt.tokens:
                if isinstance(token, Where):
                    for condition in token.tokens:
                        if isinstance(condition, Identifier):
                            where_columns.append(condition.get_real_name())
                        if isinstance(condition, Comparison):
                            for breakdown in condition.tokens:
                                if isinstance(breakdown, Identifier):
                                    where_columns.append(breakdown.get_real_name())
        return list(set(where_columns))
