
TokenDirectory = "/home/kasun/PycharmProjects/API_Tokens/"

def getWebhoseAPI():
    filename = TokenDirectory + 'webhose.txt'
    file = open(filename,'r')
    return (file.read().strip())

#getWebhoseAPI()