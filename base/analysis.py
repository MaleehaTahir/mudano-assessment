from pyspark.sql import SparkSession
from datetime import datetime
import argparse

load_date = datetime.today().strftime('%Y-%m-%d')


def create_derived_views(spark: SparkSession, country_output: str, gdp_output: str):
    spark.read.csv(country_output, header=True).createOrReplaceTempView("country")
    spark.read.csv(gdp_output, header=True).createOrReplaceTempView("country_gdp")


def get_row_count(spark: SparkSession):
    get_row_count = spark.sql("""SELECT COUNT(*) row_count from country""").collect()[0]
    return get_row_count


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
            .master("local[1]")
            .appName("test_tractable")
            .config("spark.executorEnv.PYTHONHASHSEED", "0")
            .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='Mudano test')
    parser.add_argument('--country_output', required=False,
                        default="../outputs/export/" + str(load_date).replace('-', '/') + "/country.csv")
    parser.add_argument('--gdp_output', required=False,
                        default="../outputs/export/" + str(load_date).replace('-', '/') + "/countryGDP.csv")
    args = vars(parser.parse_args())

    create_derived_views(spark_session, args['country_output'], args['gdp_output'])

    # ok, lets answer the sql questions
    # 1. countries with income level of 'Upper middle income'
    spark = spark_session
    countries_upm = spark.sql("SELECT name AS CountryName FROM country WHERE incomeLevelvalue = 'Upper middle income'")
    # print(countries_upm.show(100, False))

    # 2. List countries with income level of "Low income" per region.
    countries_lli = spark.sql("""
                SELECT regionvalue AS Region, name AS Country
                FROM country
                WHERE incomeLevelvalue = 'Low income'
                GROUP BY regionvalue, name
    """)
    # print(countries_lli.show(1000, False))

    # 3. Find the region with the highest proportion of "High income" countries.
    countries_hic = spark.sql("""
            WITH X AS(
            SELECT regionvalue AS Region, rank() over (order by count(name) desc) AS rank_countries
            FROM country
            WHERE incomeLevelvalue = 'High income'
            GROUP BY regionvalue
            )
            select Region from X
            WHERE rank_countries = 1
    """)
    # print(countries_hic.show())

    # 4. Calculate cumulative/running value of GDP per region ordered by income from
    # lowest to highest and country name.
    running_total = spark.sql("""
                   select
                        Year,
                        region,
                        IncomeValue,
                        CountryName,
                        sum(GDPValue) over(
                            partition by region, year
                            order by year, region
                        ) cumulative_sum
                    from (
                    select
                        cc.regionvalue AS Region,
                        cc.incomelevelvalue AS IncomeValue,
                        c.CountryName,
                        c.Year,
                        CAST(CAST(GDPValue AS FLOAT) AS NUMERIC(16,4)) GDPValue
                    from country_gdp c
                    JOIN country cc
                    ON c.CountryCode = cc.id
                    WHERE Year >= 2017
                    ) Y
                    --WHERE Region = 'South Asia'
                    ORDER BY Year, IncomeValue, CountryName
    """)
    # print(running_total.show(1000, False))

    # 5. Calculate percentage difference in value of GDP year-on-year per country.
    perc_change = spark.sql("""
            select s.Country, s.Year, s.GDPValue, ((GDPValue - prev_value) / (prev_value * 1.0))*100 AS PercChange
            from (select d.*,
                         lag(GDPValue) over (partition by Country order by year) as prev_value
                  from (
            SELECT CountryName AS Country,
              Year,
              CAST(CAST(GDPValue AS FLOAT) AS NUMERIC(16,4)) GDPValue
            from  country_gdp
            ) d
                 ) s
            --WHERE Country = 'United Kingdom'
            ORDER BY Country, Year, PercChange
    """)
    # print(perc_change.show(100, False))

    lowest_ranked = spark.sql("""
                    select      Region,
                                CountryName,
                                GDPValue,
                                ranking
                                from
                    (
                        select  Region,
                                CountryName,
                                GDPValue,
                                rank() over(partition by Region order by sum(GDPValue) asc) as ranking
                        from (
                            select
                            cc.regionvalue AS Region,
                            c.CountryName,
                            CAST(CAST(GDPValue AS FLOAT) AS NUMERIC(16,0)) GDPValue
                            from country_gdp c
                            JOIN country cc
                            ON c.CountryCode = cc.id
                            WHERE Year = 2017
                            --AND regionvalue = 'Middle East & North Africa'
                            ) T
                        group by region,
                                CountryName,
                                GDPValue       
                    ) temp
            where ranking BETWEEN 1 AND 3
    """)
    # print(lowest_ranked.show(30, False))
