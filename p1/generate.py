import random
from english_words import english_words_set

FILE_NUM = 99
WORD_LEN = 200_000
WORD_SAMPLES = len(english_words_set)

words = random.sample(english_words_set, WORD_SAMPLES)
file_prefix = "test"


for i in range(FILE_NUM):
    with open(f"{file_prefix}_{i}.txt", "a") as file1:
        str = ""
        for x in range(WORD_LEN):
            index = random.randint(0, len(words) - 1)
            str += f"{words[index]}  "
            if x % 24 == 0:
                file1.write(f'{str}\n')
                str = ""