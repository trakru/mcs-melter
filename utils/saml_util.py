def samlapi_formauth():
    import os
    import time
    import requests
    import getpass
    import base64
    import argparse
    import configparser
    import re
    import xml.etree.ElementTree as ET
    from bs4 import BeautifulSoup
    from os.path import expanduser
    from urllib.parse import urlparse, urlunparse, parse_qs
    import boto3

    ##########################################################################
    # Variables

    # region: The default AWS region that this script will connect
    # to for all API calls
    region = 'us-east-2'

    # output format: The AWS CLI output format that will be configured in the
    # saml profile (affects subsequent CLI calls)
    outputformat = 'text'

    # awsconfigfile: The file where this script will store the temp
    # credentials under the saml profile.  Relative to homedir.
    awsconfigfile = '/.aws/credentials'

    # SSL certificate verification: Whether or not strict certificate
    # verification is done, False should only be used for dev/test
    sslverification = True

    # idpentryurl: The initial url that starts the authentication process.
    # idpentryurl = 'https://<fqdn>:<port>/idp/profile/SAML2/Unsolicited/SSO?providerId=urn:amazon:webservices'
    idpentryurl = 'https://ama01.arrisi.com/nidp/app/login?id=SalesForce&sid=0&option=credential&sid=0&target=https://ama01.arrisi.com/nidp/saml2/idpsend?id=AWS2'


    # Uncomment to enable low level debugging
    # logging.basicConfig(level=logging.DEBUG)

    ##########################################################################

    def die(msg):
        print(msg)
        print("Attempt to login via browser to troubleshoot the issue:")
        print(idpentryurl)
        exit(1)


    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='ARRS username')
    parser.add_argument('-p', '--password', help='ARRS password')
    parser.add_argument('-r', '--role', help='AWS role ARN')
    parser.add_argument('-d', '--duration', default=3600,
                        help='Duration of temporary credentials in seconds.  Default is 3600 (1 hour).')
    parser.add_argument('-l', '--list', action='store_true', help='List available roles.  Don\'t assume role.')
    args = parser.parse_args()

    # If not given as arugment, get the federated credentials from the user
    username = args.user
    if username is None:
        #print("Username:"),
        username = input("Username: ")

    password = args.password
    if password is None:
        password = getpass.getpass()
        print('')

    # Initiate session handler
    session = requests.Session()

    # Programmatically get the SAML assertion
    # Opens the initial IdP url and follows all of the HTTP302 redirects, and
    # gets the resulting login page
    formresponse = session.get(idpentryurl, verify=sslverification)
    # Capture the idpauthformsubmiturl, which is the final url after all the 302s
    idpauthformsubmiturl = formresponse.url
    # print(idpauthformsubmiturl)
    # Parse the response and extract all the necessary values
    # in order to build a dictionary of all of the form values the IdP expects
    formsoup = BeautifulSoup(formresponse.content, "html.parser")
    payload = {}

    for inputtag in formsoup.find_all(re.compile('(INPUT|input)')):
        name = inputtag.get('name', '')
        value = inputtag.get('value', '')
        if "ecom_user_id" in name.lower():
            # Make an educated guess that this is the right field for the username
            payload[name] = username
        elif "email" in name.lower():
            # Some IdPs also label the username field as 'email'
            payload[name] = username
        elif "ecom_password" in name.lower():
            # Make an educated guess that this is the right field for the password
            payload[name] = password
        else:
            # Simply populate the parameter with the existing value (picks up hidden fields in the login form)
            payload[name] = value

    # Debug the parameter payload if needed
    # Use with caution since this will print sensitive output to the screen
    # pprint.pprint(payload)

    # If the action tag doesn't exist, we just stick with the
    # idpauthformsubmiturl above
    for inputtag in formsoup.find_all(re.compile('(FORM|form)')):
        action = inputtag.get('action')
        if action:
            idpauthformsubmiturl = action

    # Performs the submission of the IdP login form with the above post data
    response = session.post(
        idpauthformsubmiturl, data=payload, verify=sslverification)

    # Overwrite and delete the credential variables, just for safety
    username = '##############################################'
    password = '##############################################'
    del username
    del password

    # The ARRIS idp is NetIQ Access Manager.  After the form auth, it redirects
    # to an intersite transfer url defined in the initial url target parameter.
    parsedurl = urlparse(idpentryurl)
    parsedquery = parse_qs(parsedurl.query)
    intersiteurl = parsedquery['target'][0]

    # heck for login failure response
    if re.search('Login failed', response.text):
        die('Login failed')
    # look for javascript redirect to intersite url and assume error if not present
    elif not re.search(re.escape("top.location.href='%s'" % intersiteurl), response.text):
        # print(response.text)
        die('Unknown failure')

    # redirect to intersite transfer url
    response = session.get(intersiteurl, verify=sslverification)

    # Decode the response and extract the SAML assertion
    # soup = BeautifulSoup(response.text.decode('utf8'), "html.parser")
    soup = BeautifulSoup(response.text, "html.parser")
    assertion = ''

    # Look for the SAMLResponse attribute of the input tag (determined by
    # analyzing the debug print lines above)
    for inputtag in soup.find_all('input'):
        if (inputtag.get('name') == 'SAMLResponse'):
            assertion = inputtag.get('value')

    # error if missing SAMLResponse
    if (assertion == ''):
        die('Response did not contain a valid SAML assertion')

    # Debug only
    # print(base64.b64decode(assertion))

    # Parse the returned assertion and extract the authorized roles
    awsroles = []
    root = ET.fromstring(base64.b64decode(assertion))
    for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
        if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
            for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
                awsroles.append(saml2attributevalue.text)

    # Note the format of the attribute value should be role_arn,principal_arn
    # but lots of blogs list it as principal_arn,role_arn so let's reverse
    # them if needed
    for awsrole in awsroles:
        chunks = awsrole.split(',')
        if 'saml-provider' in chunks[0]:
            newawsrole = chunks[1] + ',' + chunks[0]
            index = awsroles.index(awsrole)
            awsroles.insert(index, newawsrole)
            awsroles.remove(awsrole)

    # select first role by default
    selectedroleindex = 0

    # if role specific on command line
    if args.role:
        for awsrole in awsroles:
            if re.search(re.escape(args.role), awsrole):
                selectedroleindex = awsroles.index(awsrole)
                break
    # else if there's more than one role, ask the user to choose
    elif len(awsroles) > 1:
        print("")
        i = 0
        print("Please choose the role you would like to assume:")
        for awsrole in awsroles:
            print('[', i, ']: ', awsrole.split(',')[0])
            i += 1
        # exit if --list only
        if args.list:
            exit(0)
        #print("Selection: ")
        selectedroleindex = input("Selection: ")

        # Basic sanity check of input
        if int(selectedroleindex) > (len(awsroles) - 1):
            print('You selected an invalid role index, please try again')
            exit(1)

    # set role and principle arn
    role_arn = awsroles[int(selectedroleindex)].split(',')[0]
    principal_arn = awsroles[int(selectedroleindex)].split(',')[1]

    # Use the assertion to get an AWS STS token using Assume Role with SAML
    client = boto3.client('sts', region_name=region)

    # conn = boto3.sts.connect_to_region(region)
    token = client.assume_role_with_saml(RoleArn=role_arn,
                                        PrincipalArn=principal_arn,
                                        SAMLAssertion=assertion,
                                        DurationSeconds=args.duration)
    # Write the AWS STS token into the AWS credential file
    home = expanduser("~")
    filename = home + awsconfigfile

    # Read in the existing config file
    config = configparser.RawConfigParser()
    config.read(filename)

    # Put the credentials into a saml specific section instead of clobbering
    # the default credentials
    if not config.has_section('saml'):
        config.add_section('saml')

    # print('Credentials: {}'.format(token['Credentials']))

    config.set('saml', 'output', outputformat)
    config.set('saml', 'region', region)
    config.set('saml', 'aws_access_key_id', token['Credentials']['AccessKeyId'])
    config.set('saml', 'aws_secret_access_key', token['Credentials']['SecretAccessKey'])
    config.set('saml', 'aws_session_token', token['Credentials']['SessionToken'])


    # Write the updated config file
    if os.path.exists(filename):
        os.rename(filename, '{}.bak.{}'.format(filename, (int)(time.time())))

    configdir = '{}/.aws'.format(home)
    if not os.path.exists(configdir):
        os.mkdir(configdir)

    with open(filename, 'w+') as configfile:
        config.write(configfile)

if __name__=='__main__':
    samlapi_formauth()
    print("\n New credentials generated and stored in saml profile \n")