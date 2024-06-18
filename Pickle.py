import pickle

mydict = {'dbname': '<YOUR DB NAME>',
          'dbuser': '<YOUR DB USER>',
          'dbpassword': '<YOUR DB PSSWD>',
          'dbhost': '<YOUR DB SERVER NAME>',  # Can be localhost if Postgres is running locally
          'logstable': 'sar',
          'geolocatetable': 'geolocation',
          'logFileDir': 'C:\\LG\\SERVIR\\Code\\IISLogger',
          'IPDBPathFile': 'C:\\LG\\SERVIR\\Code\\IISLogger\\IPDB\\GeoLite2-Country\\GeoLite2-Country.mmdb',
          'ReportPath': 'C:\\LG\\SERVIR\\Code\\IISLogger\\'
          }
output = open('config.pkl', 'wb')
pickle.dump(mydict, output)
output.close()
