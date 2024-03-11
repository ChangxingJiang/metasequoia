from typing import List, Any


def to_markdown_table(data: List[List[Any]]):
    """将二维数据转化为 markdown 列表（首行为标题）"""
    n_column = max(len(item) for item in data)  # 列数为最大列数

    def format_row(row):
        formatted = [str(element) for element in row]
        if len(formatted) < n_column:
            formatted.extend([""] * (n_column - len(formatted)))
        return formatted

    # 添加标题行
    result = ["| " + " | ".join(format_row(data[0])) + " |",
              "| " + " | ".join(["----"] * n_column) + " |"]

    # 添加数据行
    for item in data[1:]:
        result.append("| " + " | ".join(format_row(item)) + " |")

    return "\n".join(result)
