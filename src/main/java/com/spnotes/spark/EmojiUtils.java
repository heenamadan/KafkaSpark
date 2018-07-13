package com.spnotes.spark;

import com.vdurmont.emoji.EmojiParser;

import java.util.List;
import java.util.stream.Collectors;

public class EmojiUtils extends EmojiParser {

	public static List<String> extractEmojisAsString(String original) {
		EmojiParser parser = new EmojiParser();
		return parser.getUnicodeCandidates(original)
					.stream().map(uc -> uc.getEmoji().getUnicode())
						.collect(Collectors.toList());

	}

}
