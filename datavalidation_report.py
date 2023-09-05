import pandas as pd
from IPython.display import display, HTML

# for test report

def validation_report(src_data,stage_data,validation_data,log):
    log.info("Generating data validation report")
    html = "<html>"
    html += '''
<head>
<style>
        /* Body Styles */
        body {
            font-family: Arial, sans-serif;
            background-color: #f0f0f0;
            margin: 0;
            padding: 0;
        }

        /* Header Styles */
        header {
            background-color: #333333;
            color: #fff;
            text-align: center;
            padding: 20px;
        }



        /* Card Styles */
        .block {
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            margin: 20px;
            padding: 20px;
            justify-content: center;
            align-items: center;
            text-align: center;
        }



        /* Typography Styles */
        h1{
        color: #aab;
        }
        h2, h3 {
            color: #333;
        }



        p {
            color: #667;
        }

        table {
            background-color: #f0f0f0;
            colour: #333333
            font-family : Arial
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            margin: auto;
            padding: auto;
            justify-content: center;
            align-items: center;
            text-align: center;
        }
        th {
            font : Arial
        }
</style>
</head>



<body>
<header>
<h1>IPAD- Integration Pipeline Automation of Data</h1>
</header>'''

#### Source DIV
    html += '''<div class="block">
<h2>SOURCE</h2>
<p> Raw data from different source </p>
<br>'''

# Add a table to display the dictionary
    html += "<table border='1'><tr><th>Validation Type</th><th>Value</th></tr>"

# Iterate through the dictionary and add rows to the table
    for key, value in src_data.items():
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html += """</table></div>"""

#### Stage DIV
    html += '''<div class="block">
<h2>STAGING</h2>
<p> Staged data After ingestion from snowflake </p>
<br>'''

# Add a table to display the dictionary
    html += "<table border='1'><tr><th>Validation Type</th><th>Value</th></tr>"

# Iterate through the dictionary and add rows to the table
    for key, value in stage_data.items():
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html += """</table></div>"""
    #### comparison DIV
    html += '''<div class="block">
    <h2>CHECKPOINTS</h2>
    <p> Source vs Staging </p>
    <br>'''

    # Add a table to display the dictionary
    html += "<table border='1'><tr><th>Validation Type</th><th>Value</th></tr>"

    # Iterate through the dictionary and add rows to the table
    for key, value in validation_data.items():
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html += """</table></div>"""

    ### END of divs
### END of divs

    html += """
</body>
</html>"""
    with open("C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/reports/Aggre_data.html", "w") as f:
        f.write(html)