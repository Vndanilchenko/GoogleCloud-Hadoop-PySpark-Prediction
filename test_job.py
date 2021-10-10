# импортируем функции
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("test pyspark") \
    .master("yarn") \
    .getOrCreate()

# читаем данные
df = spark.read.csv('/user/vndanilchenko/train.csv', header=True)

# заполним все na значениями

# None
cols_na2none = ['Alley', 'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinType2', 'BsmtQual', 'Exterior2nd', 'Fence', 'FireplaceQu', 'GarageCond', 'GarageFinish', 
                'GarageQual', 'GarageType', 'MasVnrType', 'MiscFeature', 'PoolQC', ]

for col_ in cols_na2none:
    df = df.na.fill({col_:"None"})

# 0
cols_na2zero = ['BsmtFullBath', 'BsmtHalfBath', 'BsmtUnfSF', 'BsmtFinSF1', 'BsmtFinSF2', 'TotalBsmtSF', 'GarageArea', 'GarageCars', 'GarageYrBlt', 'LotFrontage', 'MasVnrArea']    

for col_ in cols_na2zero:
    df = df.na.fill({col_:0})
    
# SBrkr
df = df.na.fill({"Electrical": "SBrkr"})

# VinylSd
df = df.na.fill({"Exterior1st": "VinylSd"})

# Typ
df = df.na.fill({"Functional": "Typ"})

# TA
df = df.na.fill({"KitchenQual": "TA"})

# RL
df = df.na.fill({"MSZoning": "RL"})

# WD
df = df.na.fill({"SaleType": "WD"})

# AllPub
df = df.na.fill({"Utilities": "AllPub"})


# сделаем необходимые замены значений
df_m = df.replace("Grvl", '1', subset=['Alley']).replace("Pave", '2', subset=['Alley']).replace("NA", '0', subset=['Alley'])
df_m = df_m.replace("Po", '1', subset=['BsmtCond']).replace("Fa", '2', subset=['BsmtCond']).replace("TA", '3', subset=['BsmtCond']).replace("Gd", '4', subset=['BsmtCond']) \
    .replace("Ex", '5', subset=['BsmtCond']).replace("NA", '0', subset=['BsmtCond'])
df_m = df_m.replace("No", '2', subset=['BsmtExposure']).replace("Mn", '2', subset=['BsmtExposure']).replace("Av", '3', subset=['BsmtExposure']).replace("Gd", '4', subset=['BsmtExposure']).replace("NA", '0', subset=['BsmtExposure'])
df_m = df_m.replace("Unf", '1', subset=['BsmtFinType1']).replace("LwQ", '2', subset=['BsmtFinType1']).replace("Rec", '3', subset=['BsmtFinType1']).replace("BLQ", '4', subset=['BsmtFinType1']) \
    .replace("ALQ", '5', subset=['BsmtFinType1']).replace("GLQ", '6', subset=['BsmtFinType1']).replace("NA", '0', subset=['BsmtFinType1'])
df_m = df_m.replace("Unf", '1', subset=['BsmtFinType2']).replace("LwQ", '2', subset=['BsmtFinType2']).replace("Rec", '3', subset=['BsmtFinType2']).replace("BLQ", '4', subset=['BsmtFinType2']) \
    .replace("ALQ", '5', subset=['BsmtFinType2']).replace("GLQ", '6', subset=['BsmtFinType2']).replace("NA", '0', subset=['BsmtFinType2'])
df_m = df_m.replace("Po", '1', subset=['BsmtQual']).replace("Fa", '2', subset=['BsmtQual']).replace("TA", '3', subset=['BsmtQual']).replace("Gd", '4', subset=['BsmtQual']) \
    .replace("Ex", '5', subset=['BsmtQual']).replace("NA", '0', subset=['BsmtQual'])
df_m = df_m.replace("N", '1', subset=['CentralAir']).replace("Y", '2', subset=['CentralAir']).replace("NA", '0', subset=['CentralAir'])
df_m = df_m.replace("Po", '1', subset=['ExterCond']).replace("Fa", '2', subset=['ExterCond']).replace("TA", '3', subset=['ExterCond']).replace("Gd", '4', subset=['ExterCond']) \
    .replace("Ex", '5', subset=['ExterCond']).replace("NA", '0', subset=['ExterCond'])
df_m = df_m.replace("Po", '1', subset=['ExterQual']).replace("Fa", '2', subset=['ExterQual']).replace("TA", '3', subset=['ExterQual']).replace("Gd", '4', subset=['ExterQual']) \
    .replace("Ex", '5', subset=['ExterQual']).replace("NA", '0', subset=['ExterQual'])
df_m = df_m.replace("MnWw", '1', subset=['Fence']).replace("GdWo", '2', subset=['Fence']).replace("MnPrv", '3', subset=['Fence']).replace("GdPrv", '4', subset=['Fence']).replace("NA", '0', subset=['Fence'])
df_m = df_m.replace("Po", '1', subset=['FireplaceQu']).replace("Fa", '2', subset=['FireplaceQu']).replace("TA", '3', subset=['FireplaceQu']).replace("Gd", '4', subset=['FireplaceQu']) \
    .replace("Ex", '5', subset=['FireplaceQu']).replace("NA", '0', subset=['FireplaceQu'])
df_m = df_m.replace("Sal", '1', subset=['Functional']).replace("Sev", '2', subset=['Functional']).replace("Maj2", '3', subset=['Functional']).replace("Maj1", '4', subset=['Functional']) \
    .replace("Mod", '5', subset=['Functional']).replace("Min2", '6', subset=['Functional']).replace("Min1", '7', subset=['Functional']).replace("Typ", '8', subset=['Functional']).replace("NA", '0', subset=['Functional'])
df_m = df_m.replace("Po", '1', subset=['GarageCond']).replace("Fa", '2', subset=['GarageCond']).replace("TA", '3', subset=['GarageCond']).replace("Gd", '4', subset=['GarageCond']) \
    .replace("Ex", '5', subset=['GarageCond']).replace("NA", '0', subset=['GarageCond'])
df_m = df_m.replace("Po", '1', subset=['GarageQual']).replace("Fa", '2', subset=['GarageQual']).replace("TA", '3', subset=['GarageQual']).replace("Gd", '4', subset=['GarageQual']) \
    .replace("Ex", '5', subset=['GarageQual']).replace("NA", '0', subset=['GarageQual'])
df_m = df_m.replace("Unf", '1', subset=['GarageFinish']).replace("RFn", '2', subset=['GarageFinish']).replace("Fin", '3', subset=['GarageFinish']).replace("NA", '0', subset=['GarageFinish'])
df_m = df_m.replace("Po", '1', subset=['HeatingQC']).replace("Fa", '2', subset=['HeatingQC']).replace("TA", '3', subset=['HeatingQC']).replace("Gd", '4', subset=['HeatingQC']) \
    .replace("Ex", '5', subset=['HeatingQC']).replace("NA", '0', subset=['HeatingQC'])
df_m = df_m.replace("Po", '1', subset=['KitchenQual']).replace("Fa", '2', subset=['KitchenQual']).replace("TA", '3', subset=['KitchenQual']).replace("Gd", '4', subset=['KitchenQual']) \
    .replace("Ex", '5', subset=['KitchenQual']).replace("NA", '0', subset=['KitchenQual'])
df_m = df_m.replace("Low", '1', subset=['LandContour']).replace("HLS", '2', subset=['LandContour']).replace("Bnk", '3', subset=['LandContour']).replace("Lvl", '4', subset=['LandContour']).replace("NA", '0', subset=['LandContour'])
df_m = df_m.replace("Sev", '1', subset=['LandSlope']).replace("Mod", '2', subset=['LandSlope']).replace("Gtl", '3', subset=['LandSlope']).replace("NA", '0', subset=['LandSlope'])
df_m = df_m.replace("IR3", '1', subset=['LotShape']).replace("IR2", '2', subset=['LotShape']).replace("IR1", '3', subset=['LotShape']).replace("Reg", '4', subset=['LotShape']).replace("NA", '0', subset=['LotShape'])
df_m = df_m.replace("N", '0', subset=['PavedDrive']).replace("P", '1', subset=['PavedDrive']).replace("Y", '2', subset=['PavedDrive']).replace("NA", '0', subset=['PavedDrive'])
df_m = df_m.replace("Fa", '1', subset=['PoolQC']).replace("TA", '2', subset=['PoolQC']).replace("Gd", '3', subset=['PoolQC']).replace("Ex", '4', subset=['PoolQC']).replace("NA", '0', subset=['PoolQC'])
df_m = df_m.replace("Grvl", '1', subset=['Street']).replace("Pave", '2', subset=['Street']).replace("NA", '0', subset=['Street'])
df_m = df_m.replace("ELO", '1', subset=['Utilities']).replace("NoSeWa", '2', subset=['Utilities']).replace("NoSewr", '3', subset=['Utilities']).replace("AllPub", '4', subset=['Utilities']).replace("NA", '0', subset=['Utilities'])


# присвоим номер сезона новому полю в зависимости от месяца
seasons = {"12" : 0, "1" : 0, "2" : 0, 
           "3" : 1, "4" : 1, "5" : 1,
           "6" : 2, "7" : 2, "8" : 2, 
           "9" : 3, "10" : 3, "11" : 3}

def check_seasons(month_):
    return seasons[month_]


udf_seasons = udf(lambda x: check_seasons(x), returnType=StringType())

df_m = df_m.withColumn("SeasonSold", udf_seasons(col("MoSold")))

# посчитаем количество лет
df_m = df_m.withColumn("YrActualAge", col("YrSold")-col("YearBuilt"))

# проведем дополнительные вычисления
df_m = df_m.withColumn("TotalSF1", col("TotalBsmtSF")+col("1stFlrSF")+col("2ndFlrSF"))
df_m = df_m.withColumn("TotalSF2", col("BsmtFinSF1")+col("BsmtFinSF2")+col("1stFlrSF")+col("2ndFlrSF"))
df_m = df_m.withColumn("AllSF", col("GrLivArea")+col("TotalBsmtSF"))
df_m = df_m.withColumn("AllFlrsSF", col("1stFlrSF")+col("2ndFlrSF"))
df_m = df_m.withColumn("AllPorchSF", col("OpenPorchSF")+col("EnclosedPorch")+col("3SsnPorch")+col("ScreenPorch"))
df_m = df_m.withColumn("TotalBath", 2*(col("FullBath")+0.5*col("HalfBath")+col("BsmtFullBath")+0.5*col("BsmtHalfBath")))

# конвертируем в integer
df_m = df_m.withColumn("TotalBath", col("TotalBath").cast(IntegerType()))

df_m = df_m.withColumn("TotalPorch", col("OpenPorchSF")+col("3SsnPorch")+col("EnclosedPorch")+col("ScreenPorch")+col("WoodDeckSF"))
df_m = df_m.withColumn("OverallScore", col("OverallQual")*col("OverallCond"))
df_m = df_m.withColumn("GarageScore", col("GarageQual")*col("GarageCond"))
df_m = df_m.withColumn("ExterScore", col("ExterQual")*col("ExterCond"))
df_m = df_m.withColumn("KitchenScore", col("KitchenAbvGr")*col("KitchenQual"))
df_m = df_m.withColumn("FireplaceScore", col("Fireplaces")*col("FireplaceQu"))
df_m = df_m.withColumn("GarageScore", col("GarageArea")*col("GarageQual"))
df_m = df_m.withColumn("PoolScore", col("PoolArea")*col("PoolQC"))

# применим конструкцию условного оператора
df_m = df_m.withColumn("hasPool", when(col("PoolArea")>0,1).otherwise(0))
df_m = df_m.withColumn("has2ndFloor", when(col("2ndFlrSF")>0,1).otherwise(0))
df_m = df_m.withColumn("hasGarage", when(col("GarageArea")>0,1).otherwise(0))
df_m = df_m.withColumn("hasBsmt", when(col("TotalBsmtSF")>0,1).otherwise(0))
df_m = df_m.withColumn("hasFireplace", when(col("Fireplaces")>0,1).otherwise(0))


df_m.write.parquet('/user/vndanilchenko/train_modified_from_job.parquet')