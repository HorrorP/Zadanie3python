from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

def get_product_category_pairs_and_unassigned_products(products, categories, product_categories):
    # Broadcast join для улучшения производительности
    broadcast_categories = broadcast(categories)

    # Соединение продуктов с категориями
    product_category_pairs = products.join(product_categories, "product_id", "left") \
                                     .join(broadcast_categories, "category_id", "left")

    # Выбор необходимых колонок и фильтрация
    product_category_pairs_result = product_category_pairs.select("product_name", "category_name")

    # Продукты без категорий
    products_without_categories = product_category_pairs.filter(col("category_id").isNull()) \
                                                       .select("product_name") \
                                                       .distinct()

    return product_category_pairs_result, products_without_categories

# Пример использования
if __name__ == "__main__":
    # Создание Spark сессии
    spark = SparkSession.builder \
        .appName("Product Category Pairs") \
        .getOrCreate()

    # Пример данных
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
        (4, "Product D")
    ]

    categories_data = [
        (1, "Category X"),
        (2, "Category Y")
    ]

    product_categories_data = [
        (1, 1),
        (1, 2),
        (2, 1),
        (3, 2)
    ]

    # Создание DataFrame
    products = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    product_categories = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

    # Вызов метода
    product_category_pairs_result, products_without_categories = get_product_category_pairs_and_unassigned_products(
        products, categories, product_categories
    )

    # Показ результатов
    product_category_pairs_result.show()
    products_without_categories.show()
