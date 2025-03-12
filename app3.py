import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector

mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    port=3306,
    password='1999',
    database='RETAIL_ORDER_ANALYSIS_MINI',
    auth_plugin='mysql_native_password'  # Explicitly set the plugin
)
print(mydb)

mycursor=mydb.cursor(buffered=True)

# Function to execute SQL queries
def execute_query(sql):
    mycursor.execute(sql)
    data = mycursor.fetchall()
    columns = [desc[0] for desc in mycursor.description]  # Automatically get column names
    return pd.DataFrame(data, columns=columns)


# Add color to the title
st.markdown("<h1 style='color: black;'>Retail Order Data Analysis</h1>", unsafe_allow_html=True)
st.markdown("<h2 style='color: black;'>SQL Queries</h2>", unsafe_allow_html=True)

# Add a colored sidebar
st.sidebar.markdown(
    """
    <style>
    .stSidebar {
        background-color: #818589; /* Gunmetal Gray */
    }
    </style>
    """,
    unsafe_allow_html=True
)
#query group selection 
query_group = st.sidebar.selectbox(
    "Category Selection:",
    ["Provided Queries", "Own Queries"]
)

query_options = {
    "Provided Queries": {
        "1. Top 10 Highest Revenue Generating Products": """ 
            SELECT 
                sd.sub_category, 
                sd.product_id, 
                SUM(sd.sale_price) AS total_revenue
            FROM 
                retail_sales sd
            JOIN 
                retail_order od 
                ON sd.product_id = od.product_id
            GROUP BY 
                sd.sub_category, 
                sd.product_id
            ORDER BY 
                total_revenue DESC
            LIMIT 10;""",
        "2. Top 5 Cities with the Highest Profit Margins": """
            SELECT 
                od.city, 
                SUM(
                    CASE 
                        WHEN sd.sale_price = 0 THEN 0 
                        ELSE ((sd.profit) / (sd.sale_price * sd.quantity)) * 100 
                    END
                ) AS profit_margin
            FROM 
                retail_sales sd 
            JOIN 
                retail_order od 
                ON sd.order_id = od.order_id 
            GROUP BY 
                od.city 
            ORDER BY 
                profit_margin DESC 
            LIMIT 5;""",
        "3. Total Discount Given for Each Category": """
            SELECT 
                category, 
                SUM(discount_amount) AS total_discount 
            FROM 
                retail_sales 
            GROUP BY 
                category 
            ORDER BY 
                total_discount DESC;""",
        "4. Average Sale Price Per Product Category": """
            SELECT 
                category, 
                AVG(sale_price) AS average_sale_price 
            FROM 
                retail_sales 
            GROUP BY 
                category;""",
        "5. Region with the Highest Average Sale Price": """
            SELECT 
                region, 
                AVG(sale_price) AS average_sale_price 
            FROM 
                retail_sales 
            JOIN 
                retail_order 
                ON retail_sales.order_id = retail_order.order_id 
            GROUP BY 
                region 
            ORDER BY 
                average_sale_price DESC 
            LIMIT 1;""",
        "6. Total Profit Per Category": """
            SELECT 
                category, 
                SUM(profit) AS total_profit 
            FROM 
                retail_sales 
            GROUP BY 
                category 
            ORDER BY 
                total_profit DESC;""",
        "7. Top 3 Segments with the Highest Quantity of Orders": """
            WITH quantity_data AS (
                SELECT 
                    segment, 
                    SUM(quantity) AS quantity_of_orders 
                FROM 
                    retail_sales 
                JOIN 
                    retail_order 
                    ON retail_sales.order_id = retail_order.order_id 
                GROUP BY 
                    segment
            ) 
            SELECT 
                segment, 
                quantity_of_orders 
            FROM 
                quantity_data 
            ORDER BY 
                quantity_of_orders DESC 
            LIMIT 3;""",
        "8. Average Discount Percentage Per Region": """
            SELECT 
                region, 
                (SUM(discount_amount) / SUM(sale_price)) * 100 AS average_discount_percentage 
            FROM 
                retail_sales 
            JOIN 
                retail_order 
                ON retail_sales.order_id = retail_order.order_id 
            GROUP BY 
                region 
            ORDER BY 
                average_discount_percentage DESC;""",
        "9. Product Category with the Highest Total Profit": """
            SELECT 
                category, 
                SUM(profit) AS total_profit 
            FROM 
                retail_sales 
            GROUP BY 
                category 
            ORDER BY 
                total_profit DESC 
            LIMIT 1;""",
        "10. Total Revenue Generated Per Year": """
            SELECT 
                year, 
                SUM(sale_price) AS total_revenue 
            FROM 
                retail_sales 
            GROUP BY 
                year 
            ORDER BY 
                year;"""
    },
    "Own Queries": {
        "1. Total Sales Amount by Region": """
            SELECT 
                region, 
                SUM(sale_price) AS total_sales 
            FROM 
                retail_order 
            JOIN 
                retail_sales 
                ON retail_order.order_id = retail_sales.order_id 
            GROUP BY 
                region;""",
        "2. Top 5 Best-Selling Products by Quantity": """
            SELECT 
                product_id, 
                SUM(quantity) AS total_quantity 
            FROM 
                retail_sales 
            GROUP BY 
                product_id 
            ORDER BY 
                total_quantity DESC 
            LIMIT 5;""",
        "3. Average Discount Percentage by Category": """
            SELECT 
                category, 
                AVG(discount_percent) AS avg_discount 
            FROM 
                retail_sales 
            GROUP BY 
                category;""",
        "4. Monthly Sales Trend for a Specific Year": """
            SELECT 
                month, 
                SUM(sale_price) AS total_sales 
            FROM 
                retail_sales 
            WHERE 
                year = 2023 
            GROUP BY 
                month 
            ORDER BY 
                month;""",
        "5. Profitability by Product": """
            SELECT 
                product_id, 
                SUM(profit) AS total_profit 
            FROM 
                retail_sales 
            GROUP BY 
                product_id 
            ORDER BY 
                total_profit DESC;""",
        "6. Number of Orders Per Shipping Mode": """
            SELECT 
                ship_mode, 
                COUNT(DISTINCT order_id) AS order_count 
            FROM 
                retail_order 
            GROUP BY 
                ship_mode;""",
        "7. Total Discount Amount by State": """
            SELECT 
                state, 
                SUM(discount_amount) AS total_discount 
            FROM 
                retail_order 
            JOIN 
                retail_sales 
                ON retail_order.order_id = retail_sales.order_id 
            GROUP BY 
                state 
            ORDER BY 
                total_discount DESC;""",
        "8. Regions with the Highest Profit Margins": """
            SELECT 
                region, 
                SUM(profit) / SUM(cost_price) AS profit_margin 
            FROM 
                retail_order 
            JOIN 
                retail_sales 
                ON retail_order.order_id = retail_sales.order_id 
            GROUP BY 
                region 
            ORDER BY 
                profit_margin DESC;""",
        "9. Sales Performance by Segment": """
            SELECT 
                segment, 
                SUM(sale_price) AS total_sales 
            FROM 
                retail_order 
            JOIN 
                retail_sales 
                ON retail_order.order_id = retail_sales.order_id 
            GROUP BY 
                segment;""",
        "10. Identify Underperforming Products with Negative Profit": """
            SELECT 
                product_id, 
                SUM(profit) AS profit 
            FROM 
                retail_sales 
            GROUP BY 
                product_id 
            HAVING 
                SUM(profit) < 0;"""
    }
}

#select your queries

filter_queries = query_options.get(query_group,{})    
if not filter_queries:
    st.error(f"No queries found:{query_group}")
query_choice = st.selectbox("Select Query", list(filter_queries.keys()))

#Filter button
if st.button("Click Here!"):
    query = filter_queries.get(query_choice, " ")
    if query:
        try:
            data = execute_query(query)
            st.success("Result!")
            st.subheader("Answer")
            st.dataframe(data)
            if query_choice == "1. Top 10 Highest Revenue Generating Products":
                st.bar_chart(data.set_index("sub_category")["total_revenue"])
            elif query_choice == "2. Top 5 Cities with the Highest Profit Margins":
                st.bar_chart(data.set_index("city")["profit_margin"])
            elif query_choice == "3. Total Discount Given for Each Category":
                st.bar_chart(data.set_index("category")["total_discount"])
            elif query_choice == "4. Average Sale Price Per Product Category":
                st.bar_chart(data.set_index("category")["average_sale_price"])
            elif query_choice == "5. Region with the Highest Average Sale Price":
                st.write("Region with the Highest Average Sale Price : ", data.iloc[0]["region"])
            elif query_choice == "6. Total Profit Per Category":
                st.bar_chart(data.set_index("category")["total_profit"])
            elif query_choice == "7. Top 3 Segments with the Highest Quantity of Orders":
                st.bar_chart(data.set_index("segment")["quantity_of_orders"])
            elif query_choice == "8. Average Discount Percentage Per Region":
                st.bar_chart(data.set_index("region")["average_discount_percentage"])
            elif query_choice == "9. Product Category with the Highest Total Profit":
                st.bar_chart(data.set_index("category")["total_profit"])
            elif query_choice == "10. Total Revenue Generated Per Year":
                st.bar_chart(data.set_index("year")["total_revenue"])
        except Exception as e:
            st.error(f"Error executing query: {e}")
    else:
        st.error("No query selected.")