from wikiartcrawler import WikiartAPI, get_artist
api = WikiartAPI()
# list of artist alias related to the art movement
artist_group = get_artist('impressionism') 
# collect all the image file from each artist in the art movement
files = []
for artist in artist_group:
  tmp = api.get_painting(artist)
  if tmp is not None:
    files += tmp