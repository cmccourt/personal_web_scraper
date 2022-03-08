import requests


def get_all_new_links_on_page(page, level):
	"""Function that retrieves all of the URL in the page"""
	links, pos, all_found = [], 0, False
	try:
		response = requests.get(page[0])
		html = response.text
	except (requests.exceptions.RequestException, ValueError):
		return links

	while not all_found:
		a_tag = html.find("<a", pos)
		if a_tag > -1:
			href = html.find('href="', a_tag + 2)
			end_href = html.find('"', href + 6)
			url = html[href + 6:end_href]
			if url[:4] != 'mail':
				if url[:7] == "http://":
					if url[-1] == "/":
						url = url[:-1]
				if url[0:4] != 'http':
					strip_url = page[0].strip()
					if url in strip_url:
						url = strip_url
					else:
						url = f"{strip_url}/{url}"
				if url not in [x[0] for x in links]:
					links.append([url, level])
			close_tag = html.find("</a>", a_tag)
			pos = close_tag + 1
		else:
			all_found = True
	return links


def getUrlLinks():
	"""Function that generates the URL graph"""
	seed_url = input("Please enter the URL: ")
	to_crawl = [[seed_url, 0]]
	url_graph = {}
	crawled = []
	max_depth = 3
	while to_crawl:
		url = to_crawl.pop()
		level = url[1]
		if level == 0:
			url_graph[url[0]] = []
			crawled.append(url)
			new_links = get_all_new_links_on_page(url, level + 1)
			if len(new_links) > 0:
				for link in new_links:
					url_graph[link[0]] = []
					to_crawl.append(link)
		if level < max_depth:
			crawled.append(url)
			if url[0] not in url_graph.keys():
				url_graph[url[0]] = []
			new_links = get_all_new_links_on_page(url, level + 1)
			if new_links > 0:
				for link in new_links:
					try:
						url_graph[url[0]].append(link)
					except (TypeError, AttributeError):
						url_graph[url[0]] = link
					if link[0] not in [x[0] for x in crawled] and link[0] not in [x[0] for x in to_crawl]:
						# link hasn't been crawled nor is it on the list to be crawled. Add the link to the to_crawl
						to_crawl.append(link)
	if len(url_graph) == 1 and url_graph[seed_url] == []:
		# Couldn't load the URL given or there wasn't any links on the page
		return None
	else:
		return url_graph
