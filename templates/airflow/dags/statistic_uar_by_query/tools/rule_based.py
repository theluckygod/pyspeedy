from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction

BAD_ACTION = set(["hãy chỉ định rõ tên bài hát", "đây là các bản nhạc chuông mình tìm được", "mình thử tìm kiếm từ khóa này cho bạn nhé", "không tìm thấy kết quả theo yêu cầu của bạn", "mình chưa nghe rõ, hãy thử hỏi lại"])
NOT_FOUND = set(["mình chưa hiểu ý bạn", "mình không hiểu câu", "không tìm thấy", "không lấy được thông tin", "xin lỗi, mình chưa hiểu câu"])
STOP_WORD = set(["mở", "bài", "hát"])

SMOOTH_FUNCTION = SmoothingFunction().method1
THRESH = 0.1

def get_action_text(action):
    if "payload" not in action:
        return ""
    if "text" in action["payload"]:
        return action["payload"]["text"]
    if "utter_text" in action["payload"]:
        return action["payload"]["utter_text"]
    return ""

def match_action_to_query(query, text):
    # sq = text
    # text = re.sub(r"\(.*\)", "", text)
    # if text != sq:
    #     print("here", text, "---", sq)
    #     input()

    reference = [[x for x in text.split() if x]]
    candidate = [x for x in query.split() if x]
    while len(candidate) > 0 and candidate[0] in STOP_WORD:
        candidate = candidate[1:]
    # score = sentence_bleu(reference, candidate, weights=(0.25, 0.5, 0.75, 1.0), smoothing_function=SMOOTH_FUNCTION)
    if len(reference[0]) >= 3 and len(candidate) >= 3:
        score = sentence_bleu(reference, candidate, weights=(1, 0, 0, 0)) * 0.25
        score += sentence_bleu(reference, candidate, weights=(0, 1, 0, 0)) * 0.5
        score += sentence_bleu(reference, candidate, weights=(0, 0, 1, 0)) * 0.75
        score += sentence_bleu(reference, candidate, weights=(0, 0, 0, 1))
        # print("bleu_score", score, query, "----", text)
        score = score / 2.5 // THRESH / 10
    else:
        # print(text, "---", query)
        # input()
        if text == query:
            score = 0.25
        elif query in text or query in text:
            score = 0.15
        else:
            score = 0
        # score = sentence_bleu(reference, candidate, weights=(1, 0, 0, 0)) * 0.25
        # print("bleu_score", score, query, "----", text)
        # score = score // THRESH / 10
    # score = score / 2.5 if score / 2.5 >= THRESH else 0
    return score

def rank_query(query, acts):
    score = 0
    if len(acts) == 0:
        return score

    if "action_code" not in acts[0] or acts[0]["action_code"] != "DisplayCard":
        return 0

    text0 = get_action_text(acts[0])
    text0 = text0.lower().strip()
    if text0 not in BAD_ACTION and all(text0[:len(nf)] != nf for nf in NOT_FOUND):
        score += 1
        # score += match_action_to_query(query, text0)

        has_plMP3_act = False
        for ac in acts:
            if "action_code" in ac and ac["action_code"] == "PlayerMP3":
                has_plMP3_act = True

                text = ""
                if "name" in ac["payload"]:
                    text = ac["payload"]["name"]
                if "artist" in ac["payload"]:
                    text += " " + ac["payload"]["artist"]
                if text == "":
                    continue
                else:
                    text = text.lower().strip()

                score += 1
                bleu_score = match_action_to_query(query, text)
                if bleu_score == 0:
                    score = 0
                else:
                    score += bleu_score
                break
        
        if not has_plMP3_act:
            try:
                text = text0.split("mình tìm thấy bài hát ")[1].split(". bạn có muốn mở bài này không")[0]
            except:
                # print(f"Except: {query} --- {text0}")
                text = ""
                
            if text != "":
                bleu_score = match_action_to_query(query, text)
                if bleu_score == 0:
                    score = 0
                else:
                    score += bleu_score
            else:
                score = 0
    
    return score


def has_action(query, acts):
    score = 0
    if len(acts) == 0:
        return score

    if "action_code" not in acts[0] or acts[0]["action_code"] != "DisplayCard":
        return 0

    text0 = get_action_text(acts[0])
    text0 = text0.lower().strip()
    if text0 not in BAD_ACTION and all(text0[:len(nf)] != nf for nf in NOT_FOUND):
        return True
    return False