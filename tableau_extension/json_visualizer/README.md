# Try Yourself
1. You need to have python installed.
2. run the `hosting.ps1` in the `tableau_extension` folder
This will start a python simple http server using the same directory as the root.
3. Open Tableau Desktop (Tested with 2023.3 only)
4. Add a new workbook connecting to your data with a column of json data. (or you can use `datasource\datasource.xlsx`)
5. Reset all the names of your columns. (Because Tableau automatically rename your fields!)
6. In a new worksheet, drag the json fields to the row panel.
7. Create a new dashboard, put the worksheet there and add a local extension using `json_visualizer.trex`
8. Configure the extension.
9. Enjoy!

![Illustration](/tableau_extension/json_visualizer/img/resultdemo.gif)