# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Display the list databricks utils

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Below dbutils is the comprehensive one, out of which we are going to concentrate currently on notebook, widgets and fs for now

# COMMAND ----------

dbutils.help()
#important utils are
#fs, jobs, notebook, widgets

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Notebook utils help

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. FS Commands

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

print("lets learn all fs commands options...")
print("copying")
dbutils.fs.cp("/Volumes/workspace/default/volumewd36/sample_healthcare_patients.csv","/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")
print("head of 10 rows")
print(dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv"))
print("listing")
dbutils.fs.ls("/Volumes/workspace/default/volumewd36/")
print("make directory")
dbutils.fs.mkdirs("/Volumes/workspace/default/volumewd36/healthcare/")
print("move")
dbutils.fs.mv("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","/Volumes/workspace/default/volumewd36/healthcare/sample_healthcare_patients1.csv")
dbutils.fs.ls("/Volumes/workspace/default/volumewd36/healthcare/")
dbutils.fs.cp("/Volumes/workspace/default/volumewd36/sample_healthcare_patients.csv","/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv")
print("put to write some data into a file")

# COMMAND ----------

print("try below command without the 3rd argument of true, you will find the dbfs-> hadoop -> spark -> s3 bucket")
#dbutils.fs.put("dbfs:///Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","put something",False)
print(dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv"))
dbutils.fs.put("dbfs:///Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv","put something",True)
print("see the data in the file")
print(dbutils.fs.head("/Volumes/workspace/default/volumewd36/sample_healthcare_patients1.csv"))
dbutils.fs.rm("/Volumes/workspace/default/volumewd36/healthcare/sample_healthcare_patients1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Widgets utils help

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Widgets utility used for adding the components/widgets into our notebook for creating
# MAGIC dynamic/parameterized approaches

# COMMAND ----------

print("can you create a textbox widget")
dbutils.widgets.text("tablename","cities","enter the tablename to query")

# COMMAND ----------

print("can you get the value of the widget using dbutils.widgets.get and store into a local python variable tblname")
tblname=dbutils.widgets.get("tablename")
print("user passed the value of ?",tblname)

# COMMAND ----------

#Implemented dynamic SQL usecase in Databricks
display(spark.sql(f"select * from default.{tblname} limit 10"))

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_widget","Senthil",["Senthil","Balaji","Arun"],"Select your name")
aspirant_name_chosen=dbutils.widgets.get("dropdown_widget")
print("Good morning",aspirant_name_chosen)

# COMMAND ----------

dbutils.widgets.multiselect("multiselect_widget","wd36",["wd32","we43","we45","wd36"],"Select your team name")
all_batches=dbutils.widgets.get("multiselect_widget")
all_batches_lst=all_batches.split(",")
for i in all_batches_lst:
    print(f"hello team {i}")
#print("You have chosen the team name as",all_batches)

# COMMAND ----------

#Interview question- how to access some value from the given string
fullname="mohamed kader irfan"
fname=fullname.split(" ")[0]
lname=fullname.split(" ")[-1]
print(fname, 'and', lname)

# COMMAND ----------

dbutils.widgets.combobox("combobox_widget","wd36",["wd32","we43","we45","wd36"],"Select your team name")
combobox_value=dbutils.widgets.get("combobox_widget")
print("Good morning",combobox_value)

# COMMAND ----------

dbutils.widgets.text("team_name","WD36","This is to represent our team name")

# COMMAND ----------

text_box_value1=dbutils.widgets.get("team_name")
print("Good Morning ",text_box_value1)

# COMMAND ----------

dbutils.widgets.dropdown("listbox","wd36",["wd32","we43","we45","wd36"],"Team names drop down")
listbox_value2=dbutils.widgets.get("listbox")
print("Good morning",listbox_value2)

# COMMAND ----------

dbutils.widgets.combobox("combobox","we47",["wd32","we43","we45","we47"],"Team names combo box")

# COMMAND ----------

dbutils.widgets.multiselect("multiselect","wd36",["wd32","we43","we45","wd36"],"Team names multiselect")

# COMMAND ----------

dict_all_widgets=dbutils.widgets.getAll()
print(dict_all_widgets)

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Calling a child notebook (example_child_notebook.ipynb) from this parent notebook with parameters
# MAGIC dbutils.widgets.text("param1", "default_value", "Your input parameter")
# MAGIC param_value = dbutils.widgets.get("param1")
# MAGIC print("printing the parameters",param_value)

# COMMAND ----------

child_return_value=dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook", 180,{"table_name":"cities1"})

# COMMAND ----------

if True:
    dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook",600)
else:
    dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook",300)

# COMMAND ----------

import time
for i in range(13):
    dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook",300)
    time.sleep(10)

# COMMAND ----------

dbutils.widgets.removeAll()
