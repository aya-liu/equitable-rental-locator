'''
CHA Find HCV Housing Listing Scraper

Aya Liu 
'''

import sys
import bs4
import re
import util
import pickle

START_URL = "http://chicagoha.gosection8.com/Tenant/tn_Results.aspx"
TEST_URL = "http://chicagoha.gosection8.com/Tenant/tn_Results.aspx?&pg=376"
TEST_URL2  = "http://chicagoha.gosection8.com/Tenant/tn_Results.aspx?&pg=370"
ROOT_URL = "http://chicagoha.gosection8.com"


def scrape(url):
    '''
    Scrape all rental listings starting from url all the way to the last page
    of the HCV housing locator.
    
    Input: url (str): the starting url
    Returns: hd (dict): rental listing dictionary, mapping listing id to the 
                    following attributes:
                - address
                - monthly rent
                - number of bed and baths
                - property type (house / apt)
                - whether voucher is necessary
                - availability
                - contact inf
                - URL for the listing
    '''

    hd = {}
    soup = url_to_soup(url)
    print(url)
    parse_listings(soup, hd)
    add_geocode(soup, hd)
    
    next_url = find_next_page(soup)
    if not next_url:
        return hd
    else:
        hd_next = scrape(next_url)
        hd.update(hd_next)
    return hd


def find_next_page(soup):
    tag = soup.find("a", class_="PagingPrevNextButton", text="Next »")
    if tag:
        url = tag["href"]
        full_url = util.convert_if_relative_url(ROOT_URL, url)
        return full_url
    else:
        return None

def parse_listings(soup, hd, debug=False):
    '''
    Parse listing info from one page.

    Inputs: 
        soup (BeautifulSoup): a BeautifulSoup object from one page
        hd (dict): rental listing dictionary
    '''
    listings = soup.find_all("div", class_=re.compile("(listing)+[0-9]+"))

    for l in listings:
        l_id = l["class"][0][7:]
        if debug:
            print(l_id)
        # get rent
        rent_tag = l.find_all("b", class_="rent")[0]
        rent = int(''.join(filter(str.isdigit, rent_tag.text)))

        # get property info
        bed_tag = l.find_all("span", id=re.compile("Main.*Bed_+[0-9]*"))[0]
        if bed_tag.text == "Efficiency":
            num_bed = 0
        else:
            num_bed = float(bed_tag.text)
        bath_tag = bed_tag.next_sibling
        num_bath = float(bath_tag.split()[1])
        ptype_tag = bath_tag.next_sibling
        ptype = ptype_tag.text
        
        # get voucher requirement
        hcv_tag = ptype_tag.parent.next_sibling.next_sibling
        if hcv_tag["class"] == "novouchernecessary":
            hcv_req = "No"
        else:
            hcv_req = "Yes"

		# get availability and contact
        avail = l.find(class_= "availability").text
        if avail == "Available Now":
            contact = l.find(class_="subbold contact-phone").text
        else:
            contact = None

        # get address and details_url
        address_tag = l.find("a", class_="address")
        address = address_tag.text
        details_url = util.convert_if_relative_url(ROOT_URL, 
                                                    address_tag["href"])

        # update dict
        hd[l_id] = {
            "Address": address,
            "Monthly Rent": rent,
            "Bed": num_bed,
            "Bath": num_bath,
            "Property Type": ptype,
            "Voucher Necessary": hcv_req,
            "Availability": avail,
            "Contact": contact,
            "URL": details_url}


def add_geocode(soup, hd):
    '''
    Add lat, long attributes to rental unit dictionary based on the 
    googleMapsAPIKey tag from the html.

    Inputs:
        soup (BeautifulSoup): a BeautifulSoup object from one page
        hd (dict): rental listing dictionary mapping listing id to attributes
    '''
    s = soup.find('script', text = re.compile(r'.*googleMapsAPI.*')).text
    sl = s.split()
    start = sl.index("[") + 3 # first listing id
    end = sl.index("];") - 4 + 1 # last listing id
    for i in range(start, end, 6):
        l_id = sl[i][1:-2]
        hd[l_id]["Lat"] = float(sl[i-2][1:-1])
        hd[l_id]["Long"] = float(sl[i-1][1:-1])


def url_to_soup(url):
    '''
    Read a url into a BeautifulSoup object.

    Input: url (str)
    Returns: 
        soup (BeautifulSoup): a BeautifulSoup object
        req_url (str): the request url
    '''
    request = util.get_request(url)
    if not request:
        print("Error - Cannot get request")
        return None
    else:
        html = util.read_request(request)
    if not html:
        print("Error - Cannot read request")
        return None
    else:
        return bs4.BeautifulSoup(html, features="html5lib")


if __name__ == '__main__':
    hd = scrape(START_URL)
    f = open(sys.argv[1],'wb')
    pickle.dump(hd, f)

