import unittest

from veryscrape import load_query_dictionary, synchronous, get_auth
from veryscrape.extensions.reddit import Reddit


@synchronous
async def f():
    return await get_auth('reddit')


class RedditTest(unittest.TestCase):
    auths = f()
    topics = load_query_dictionary('subreddits')
    auth = {k: a for k, a in zip(sorted(topics.keys()), auths)}
    topic = next(iter(list(topics.keys())))
    q = 'all'

    def test_get_links(self):
        @synchronous
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            await reddit.close()
        run()

    def test_get_comments(self):
        @synchronous
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            url = reddit.comment_url.format(self.q, resp['data']['children'][-1]['data']['id'], '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp[0]['data'], 'Invalid response'
            assert isinstance(resp[1]['data']['children'], list), 'Comments incorrectly returned'
            await reddit.close()
        run()

    def test_send_comments(self):
        @synchronous
        async def run():
            reddit = Reddit(self.auth[self.topic])
            cmnts = [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 196, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 29, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'immediately arrested', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>immediately arrested</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'mechabeast', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506725466.0, 'depth': 1, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 29, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno48wr', 'downs': 0, 'name': 't1_dnobb4u', 'id': 'dnobb4u', 'created_utc': 1506696666.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': "I don't care, I'm still free  \nYou can't take the sky from me", 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>I don&#39;t care, I&#39;m still free<br/>\nYou can&#39;t take the sky from me</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'Aevann', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506715909.0, 'depth': 0, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 196, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't3_736js6', 'downs': 0, 'name': 't1_dno48wr', 'id': 'dno48wr', 'created_utc': 1506687109.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}, {'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 63, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 66, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 25, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 19, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 16, 'replies': {'kind': 'Listing', 'data': {'whitelist_status': 'all_ads', 'modhash': '', 'before': None, 'children': [{'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 6, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'And Gina Torres (Zoe in *Firefly*) is Ikora in Destiny as well.', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>And Gina Torres (Zoe in <em>Firefly</em>) is Ikora in Destiny as well.</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'SuperFreakyNaughty', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506727752.0, 'depth': 5, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 6, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dnoal9x', 'downs': 0, 'name': 't1_dnodfph', 'id': 'dnodfph', 'created_utc': 1506698952.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}, {'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 5, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'He did some voice acting in Rick and Morty and killed it there too. I believe the first episode of the third season.', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>He did some voice acting in Rick and Morty and killed it there too. I believe the first episode of the third season.</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'StormTrooperQ', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506729021.0, 'depth': 5, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 5, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dnoal9x', 'downs': 0, 'name': 't1_dnoemul', 'id': 'dnoemul', 'created_utc': 1506700221.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'Nathan Fillion might even be interested. He has been doing the voice of Cayde-6 (and crushing it, btw) in Destiny for the last few years. ', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>Nathan Fillion might even be interested. He has been doing the voice of Cayde-6 (and crushing it, btw) in Destiny for the last few years. </p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'Kingspencer', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506724661.0, 'depth': 4, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 16, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno89r0', 'downs': 0, 'name': 't1_dnoal9x', 'id': 'dnoal9x', 'created_utc': 1506695861.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': 1506697101.0, 'removal_reason': None, 'score_hidden': False, 'body': "It wouldn't be the same without the main-cast voices, but if you could find the right actors to replace them, I could see Firefly having a pretty good second life as an animated series. Young Justice, for example, showed that animation was totally capable of tackling darker, more complex, and just *better* storylines than previously thought. And despite the perfect casting of Patrick Stewart and Robert Downey Jr., there have been quite a few voice actors who did their characters very well and even made it believable that they were the same version of the character.\n\nNathan Fillion and Ron Glass would probably be the hardest to replace.\n\nEDIT: Robert Downey Jr. is the actor, Tony Stark is the character. Not the other way around. Whoops.\n\nEDIT 2: Ron Glass portrayed Shepherd Book, not Ron Granier. Ron Granier is the composer who wrote the original 1960s Doctor Who theme. I am a mess today.", 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>It wouldn&#39;t be the same without the main-cast voices, but if you could find the right actors to replace them, I could see Firefly having a pretty good second life as an animated series. Young Justice, for example, showed that animation was totally capable of tackling darker, more complex, and just <em>better</em> storylines than previously thought. And despite the perfect casting of Patrick Stewart and Robert Downey Jr., there have been quite a few voice actors who did their characters very well and even made it believable that they were the same version of the character.</p>\n\n<p>Nathan Fillion and Ron Glass would probably be the hardest to replace.</p>\n\n<p>EDIT: Robert Downey Jr. is the actor, Tony Stark is the character. Not the other way around. Whoops.</p>\n\n<p>EDIT 2: Ron Glass portrayed Shepherd Book, not Ron Granier. Ron Granier is the composer who wrote the original 1960s Doctor Who theme. I am a mess today.</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'JackHarkness42', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506721888.0, 'depth': 3, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 19, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno7tnr', 'downs': 0, 'name': 't1_dno89r0', 'id': 'dno89r0', 'created_utc': 1506693088.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}, {'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 1, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'He appears in one of the follow up comics, if you are interested in that kind of thing. I personally found them to be very true to the tone of the show.', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>He appears in one of the follow up comics, if you are interested in that kind of thing. I personally found them to be very true to the tone of the show.</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'AustinPowers', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506728683.0, 'depth': 3, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 1, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno7tnr', 'downs': 0, 'name': 't1_dnoebd4', 'id': 'dnoebd4', 'created_utc': 1506699883.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': "Getting to see his character expanded upon is one of the top things I would've looked forward to in having the series continue. I imagine him becoming something akin to a hybrid of Boba Fett and Ledger's Joker. ", 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>Getting to see his character expanded upon is one of the top things I would&#39;ve looked forward to in having the series continue. I imagine him becoming something akin to a hybrid of Boba Fett and Ledger&#39;s Joker. </p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'vatreehugger', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506721312.0, 'depth': 2, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 25, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno64cj', 'downs': 0, 'name': 't1_dno7tnr', 'id': 'dno7tnr', 'created_utc': 1506692512.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'Jubal Early was one of the most interesting walk-on characters of the day. ', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>Jubal Early was one of the most interesting walk-on characters of the day. </p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'flagcaptured', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506718974.0, 'depth': 1, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 66, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno5sej', 'downs': 0, 'name': 't1_dno64cj', 'id': 'dno64cj', 'created_utc': 1506690174.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}, {'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 2, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'I said alliance.', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>I said alliance.</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'sad_heretic', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506724068.0, 'depth': 1, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 2, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno5sej', 'downs': 0, 'name': 't1_dnoa2l8', 'id': 'dnoa2l8', 'created_utc': 1506695268.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}, {'kind': 't1', 'data': {'subreddit': 'firefly', 'distinguished': None, 'approved_at_utc': None, 'banned_by': None, 'ups': 1, 'replies': '', 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'I have a mighty *roar* ', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>I have a mighty <em>roar</em> </p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'caskaziom', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506726884.0, 'depth': 1, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 1, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't1_dno5sej', 'downs': 0, 'name': 't1_dnocmlw', 'id': 'dnocmlw', 'created_utc': 1506698084.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}], 'after': None}}, 'controversiality': 0, 'link_id': 't3_736js6', 'stickied': False, 'edited': False, 'removal_reason': None, 'score_hidden': False, 'body': 'Yes, I suppose I am a Lion', 'can_gild': True, 'collapsed_reason': None, 'body_html': '<div class="md"><p>Yes, I suppose I am a Lion</p>\n</div>', 'is_submitter': False, 'gilded': 0, 'author_flair_text': None, 'author': 'WorldOfPayne', 'subreddit_type': 'public', 'banned_at_utc': None, 'created': 1506718478.0, 'depth': 0, 'num_reports': None, 'archived': False, 'user_reports': [], 'mod_reports': [], 'author_flair_css_class': None, 'collapsed': False, 'score': 63, 'subreddit_name_prefixed': 'r/firefly', 'can_mod_post': False, 'parent_id': 't3_736js6', 'downs': 0, 'name': 't1_dno5sej', 'id': 'dno5sej', 'created_utc': 1506689678.0, 'report_reasons': None, 'subreddit_id': 't5_2qs24', 'likes': None, 'approved_by': None, 'saved': False}}]
            await reddit.send_comments(cmnts, self.topic)
            await reddit.close()
        run()

if __name__ == '__main__':
    unittest.main()
