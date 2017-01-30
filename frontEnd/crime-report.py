#!/usr/bin/env python
'''
This code is a CGI interface with crime-reports.html. 
Please run requirements.txt on your web server first. 
'''
import cgi, cgitb
from starbase import Connection

form = cgi.FieldStorage()
community = form.getfirst('community', 'empty')
crime = form.getfirst('crimeType', 'empty')

def main(community, crime):
    crime_columns = ['year', 'num_homicides','num_robberies',
        'num_batteries', 'num_assaults', 'num_burglaries', 
        'num_thefts', 'num_narcotics', 'num_other']
    weather_columns = ['meanTemp', 'clear',
        'rain', 'snow', 'fog']
    crime_column_names = ['Year', 'Homicides', 'Robberies', 'Batteries', 'Assaults',
        'Burglaries', 'Thefts', 'Narcotics', 'Other']
    weather_column_names = ['','Average Temperature', 'Percent Clear Days',
        'Percent Rainy Days', 'Percent Snowy Days', 'Percent Fog']
    crime_dict = {'HOMICIDE': 'Homicides', 'ASSAULT':'Assaults', 
        'BURGLARY':'Burglaries', 'BATTERY':'Batteries', 
        'NARCOTICS':'Narcotics', 'THEFT': 'Thefts', 'ROBBERY': 'Robberies', 
        'OTHER':'Other'}

    connection = Connection(host='hdp-m.c.mpcs53013-2016.internal', port='2056')

    comm_data = []
    yoy_data = []
    crime_data = []

    # Make sure inputs are sanitized and type safe to prevent code injection

    ## GETS COMMUNITY DATA OVER TIME FOR ALL CRIMES##
    if community != 'empty' and (crime == 'empty' or crime == 'all'):
        table = connection.table('agolab_summed_crimes_by_community')
        table_out = [crime_column_names]
        # Get Data from Hbase for each year for Community
        for year in range(2006,2017):
            comm_data.append(table.fetch(b'%s-%s' % (community, year)))
        # Assemble a text table
        for i in range(len(comm_data)):
            add = []
            add.append('%s' % range(2006,2017)[i])
            for j in range(1, len(crime_columns)):
                add.append(comm_data[i]['crime'][crime_columns[j]])
            table_out.append(add)

    ## GETS COMMUNITY DATA OVER TIME FOR PARTICULAR CRIME ##
    elif community != 'empty' and crime != 'empty': 
        table = connection.table('agolab_summed_crimes_by_community')
        table_out = [['Year', crime_dict[crime]]]
        # Get Data from Hbase for each year for Community
        for year in range(2006, 2017):
            yoy_data.append(table.fetch(b'%s-%s' % (community, year), 
                [b'crime:num_%s' % (crime_dict[crime].lower())]))
        # Assemble a text table
        for i in range(len(yoy_data)):
            add = []
            add.append('%s' % range(2006,2017)[i])
            add.append(yoy_data[i]['crime']['num_%s' % (crime_dict[crime].lower())])
            table_out.append(add)

    elif community == 'empty' and crime != 'empty':
        table = connection.table('agolab_crime_weather_sums')
        # Get weather data for this crime
        crime_data.append(table.fetch(b'%s' % (crime)))
        table_out = [weather_column_names]
        count = float(crime_data[0]['weather']['count'])
        add=[crime_dict[crime] + ' Weather']
        for key in weather_columns:
            value = float(crime_data[0]['weather'][key])
            if key == 'meanTemp':
                add.append('{:.2f}'.format(value))
            else:
                add.append('{:.2f}%'.format(value/count * 100))
        table_out.append(add)
        
        # Get chicago weather data
        table = connection.table('agolab_ave_chicago_weather')
        ave_data = []
        ave_data.append(table.fetch(b'%s' % 'Chicago'))
        print "AVEDATA: ", ave_data
        add = ['Chicago Average']
        count = float(ave_data[0]['ave']['count'])
        for key in weather_columns:
            if key == 'meanTemp':
                value = float(ave_data[0]['ave']['meanTemperature'])
                add.append('{:.2f}'.format(value))
            elif key == 'now':
                value = float(ave_data[0]['ave']['snow'])
                add.append('{:.2f}%'.format(value/count * 100))
            else:
                value = float(ave_data[0]['ave'][key])
                add.append('{:.2f}%'.format(value/count * 100))
        table_out.append(add)
            
    width = 100/len(table_out[0])

    ## PRINT THE HTML    

    print 'Content-type: text/html\n\n'
    print '<html><head> <title>HBase Output</title> </head><body>'
    if len(crime_data) == 0:
        print '''<div align="center" style="padding:10px;color:white;font-size:16px;
            font-family:arial;font-weight:bold;border: 1px solid white;
            margin-right:25%;margin-left:25%;margin-top:10px;margin-bottom:20px;
            background-color:gray;">&nbsp;Crimes by category for {} since 2011&nbsp;</div>'''.format(community)
    else:
        print '''<div align="center" style="padding:10px;color:white;font-size:16px;
            font-family:arial; font-weight:bold; border: 1px solid white;
            margin-right:25%;margin-left:25%; margin-top:10px;margin-bottom:20px;
            background-color:gray;">&nbsp;Weather stats for all {} since 2001&nbsp;</div>'''.format(crime_dict[crime])
    print '<p style="bottom-margin:10px"/>';
    html_table(table_out, width)
    print '<p style="bottom-margin:10px"/>'
    print '</body></html>'

### ERROR MESSAGE FOR NO VALUE, ELSE RUN ###

def html_table(array, width):
    '''
    Creates an html table out of nested lists.
    SOURCE: http://stackoverflow.com/questions/1475123/easiest-way-to-turn-a-list-into-an-html-table-in-python
    Many thanks to Alex Martelli.
    '''
    print '<table width="75%" cellspacing="8" cellpadding="0" border="0" align="center" bgcolor="#999999">'
    count = 0
    for sublist in array:
        if count == 0:
            print '<tr bgcolor="#ffffff"><td height="30" width="{}%" style="font-family:arial;font-weight:bold;">'.format(width)
            print '</td><td height="30" width="{}%" style="font-family:arial;font-weight:bold;">'.format(width).join(sublist)
            print '</td></tr>'
            count += 1
        else: 
            print '<tr bgcolor="#ffffff"><td height="30" width="{}%" style="font-family:arial">'.format(width)
            print '</td><td height="30" width="{}%" style="font-family:arial">'.format(width).join(sublist)
            print '</td></tr>'
    print '</table>'

if community == 'empty' and crime == 'empty':
    print 'Content-type: text/html\n\n'
    print '<html><head> <title>HBase Output</title> </head><body>'
    print '''<div align="center" style="padding:10px;color:white;font-size:16px;
    font-family:arial;font-weight:bold;border: 1px solid white;
    margin-right:25%;margin-left:25%;margin-top:10px;margin-bottom:20px;
    background-color:gray;">&nbsp;Please select a neighborhood, a crime, or both.&nbsp;</div>'''
    print '</body></html>'
else: 
    main(community, crime)


