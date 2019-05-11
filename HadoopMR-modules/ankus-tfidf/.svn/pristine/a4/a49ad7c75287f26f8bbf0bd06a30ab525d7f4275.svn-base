package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.CompoundNounAnalyzer;
import org.apache.lucene.analysis.ko.morph.MorphAnalyzer;
import org.apache.lucene.analysis.ko.morph.MorphException;
import org.apache.lucene.analysis.ko.morph.WordSegmentAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

	 

public class ArirangAnalyzerHandler {
	 Set<String> josaSet = new HashSet<String>();
	 
	  /**
	   * 문자열의 마지막 글자를 char형으로 변형
	   * @param str
	   * @return
	   */
	  private char getLastChar(String str) {
	    return str.charAt(str.length()-1);
	  }
	  /**
	   * 한글의 종성 여부를 판단
	   * @param hanAllElement
	   * @return
	   */
	  private boolean hasLastElement(int[] hanAllElement) {
	    if (hanAllElement[2] > 0) {
	      return true;
	    } else {
	      return false;
	    }
	  }
	  /**
	   * 한글 여부를 판단
	   * @param lastChar
	   * @return
	   */
	  private boolean isHangul(char lastChar) {
	      if (lastChar >= 0xAC00 &&lastChar <= 0xD7AF) {
	        return true;
	      } else {
	        return false;
	      }
	  }
	 
	  
    /** 
    * @Method Name : morphAnalyze 
    * @변경이력              : 
    * @Method 설명     : 형태소 분석
    * @param source
    * @return
    * @throws MorphException 
    */
	private Logger logger = LoggerFactory.getLogger(ArirangAnalyzerHandler.class);
	HashMap<String, Double > importantkey = null;
	private boolean use_keyword = false;
    public String morphAnalyze(String source) throws MorphException {
        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
         
        StringBuilder result = new StringBuilder();
         
        StringTokenizer stok = new StringTokenizer(source, " ");
        
        while (stok.hasMoreTokens())
        {
            String token = stok.nextToken();
            List<AnalysisOutput> outList = maAnal.analyze(token);
            for (AnalysisOutput o : outList) 
            {
            	String anaResult = o.toString();
            	System.out.println(anaResult);
                result.append(o).append(" ");
            }
        }
        return result.toString();
    }
    
    @SuppressWarnings("deprecation")
	public ArirangAnalyzerHandler(boolean keyword , Configuration conf, String path)
    {
    	josaSet.add("거나");
    	josaSet.add("거늘");
    	josaSet.add("거니");
    	josaSet.add("거니와");
    	josaSet.add("거드면");
    	josaSet.add("거드면은");
    	josaSet.add("거든");
    	josaSet.add("거들랑");
    	josaSet.add("거들랑은");
    	josaSet.add("건");
    	josaSet.add("건대");
    	josaSet.add("건댄");
    	josaSet.add("건마는");
    	josaSet.add("건만");
    	josaSet.add("것다");
    	josaSet.add("게");
    	josaSet.add("게끔");
    	josaSet.add("게나");
    	josaSet.add("게나마");
    	josaSet.add("게는");
    	josaSet.add("게도");
    	josaSet.add("게라도");
    	josaSet.add("게만");
    	josaSet.add("게만은");
    	josaSet.add("게시리");
    	josaSet.add("게요");
    	josaSet.add("고");
    	josaSet.add("고는");
    	josaSet.add("고도");
    	josaSet.add("고만");
    	josaSet.add("고말고");
    	josaSet.add("고서");
    	josaSet.add("고서는");
    	josaSet.add("고서도");
    	josaSet.add("고선");
    	josaSet.add("고야");
    	josaSet.add("고요");
    	josaSet.add("고자");
    	josaSet.add("곤");
    	josaSet.add("관데");
    	josaSet.add("구나");
    	josaSet.add("구려");
    	josaSet.add("구료");
    	josaSet.add("구먼");
    	josaSet.add("군");
    	josaSet.add("군요");
    	josaSet.add("기");
    	josaSet.add("기까지");
    	josaSet.add("기까지는");
    	josaSet.add("기까지도");
    	josaSet.add("기까지만");
    	josaSet.add("기까지만은");
    	josaSet.add("기로");
    	josaSet.add("기로서");
    	josaSet.add("기로서니");
    	josaSet.add("기로선들");
    	josaSet.add("기에");
    	josaSet.add("긴");
    	josaSet.add("길");
    	josaSet.add("나");
    	josaSet.add("나니");
    	josaSet.add("나마");
    	josaSet.add("나요");
    	josaSet.add("나이까");
    	josaSet.add("나이다");
    	josaSet.add("냐");
    	josaSet.add("냐고");
    	josaSet.add("냐는");
    	josaSet.add("냐라고");
    	josaSet.add("냐라고도");
    	josaSet.add("냐라고만");
    	josaSet.add("냐에");
    	josaSet.add("네");
    	josaSet.add("네만");
    	josaSet.add("네요");
    	josaSet.add("노");
    	josaSet.add("노라");
    	josaSet.add("노라고");
    	josaSet.add("노라니");
    	josaSet.add("노라면");
    	josaSet.add("느냐");
    	josaSet.add("느냐고");
    	josaSet.add("느냐는");
    	josaSet.add("느냐라고");
    	josaSet.add("느냐라고는");
    	josaSet.add("느냐라고도");
    	josaSet.add("느냐라고만");
    	josaSet.add("느냐라고만은");
    	josaSet.add("느냐에");
    	josaSet.add("느뇨");
    	josaSet.add("느니");
    	josaSet.add("느니라");
    	josaSet.add("느니만");
    	josaSet.add("느라");
    	josaSet.add("느라고");
    	josaSet.add("는");
    	josaSet.add("는가");
    	josaSet.add("는가라고");
    	josaSet.add("는가라는");
    	josaSet.add("는가를");
    	josaSet.add("는가에");
    	josaSet.add("는걸");
    	josaSet.add("는고");
    	josaSet.add("는구나");
    	josaSet.add("는구려");
    	josaSet.add("는구료");
    	josaSet.add("는구먼");
    	josaSet.add("는군");
    	josaSet.add("는다");
    	josaSet.add("는다거나");
    	josaSet.add("는다고");
    	josaSet.add("는다고는");
    	josaSet.add("는다는");
    	josaSet.add("는다는데");
    	josaSet.add("는다니");
    	josaSet.add("는다니까");
    	josaSet.add("는다든지");
    	josaSet.add("는다마는");
    	josaSet.add("는다만");
    	josaSet.add("는다만은");
    	josaSet.add("는다며");
    	josaSet.add("는다며는");
    	josaSet.add("는다면");
    	josaSet.add("는다면서");
    	josaSet.add("는다면은");
    	josaSet.add("는단다");
    	josaSet.add("는담");
    	josaSet.add("는답니까");
    	josaSet.add("는답니다");
    	josaSet.add("는답디까");
    	josaSet.add("는답디다");
    	josaSet.add("는답시고");
    	josaSet.add("는대");
    	josaSet.add("는대로");
    	josaSet.add("는대서");
    	josaSet.add("는대서야");
    	josaSet.add("는대야");
    	josaSet.add("는대요");
    	josaSet.add("는데");
    	josaSet.add("는데는");
    	josaSet.add("는데다");
    	josaSet.add("는데도");
    	josaSet.add("는데서");
    	josaSet.add("는만큼");
    	josaSet.add("는만큼만");
    	josaSet.add("는바");
    	josaSet.add("는지");
    	josaSet.add("는지가");
    	josaSet.add("는지고");
    	josaSet.add("는지는");
    	josaSet.add("는지도");
    	josaSet.add("는지라");
    	josaSet.add("는지를");
    	josaSet.add("는지만");
    	josaSet.add("는지에");
    	josaSet.add("는지요");
    	josaSet.add("는지의");
    	josaSet.add("니");
    	josaSet.add("니까");
    	josaSet.add("니까는");
    	josaSet.add("니깐");
    	josaSet.add("니라");
    	josaSet.add("니만치");
    	josaSet.add("니만큼");
    	josaSet.add("다");
    	josaSet.add("다가");
    	josaSet.add("다가는");
    	josaSet.add("다가도");
    	josaSet.add("다간");
    	josaSet.add("다거나");
    	josaSet.add("다고");
    	josaSet.add("다고까지");
    	josaSet.add("다고까지는");
    	josaSet.add("다고까지도");
    	josaSet.add("다고까지라도");
    	josaSet.add("다고까지만");
    	josaSet.add("다고까지만은");
    	josaSet.add("다고는");
    	josaSet.add("다고도");
    	josaSet.add("다고만");
    	josaSet.add("다고만은");
    	josaSet.add("다고요");
    	josaSet.add("다곤");
    	josaSet.add("다느냐");
    	josaSet.add("다느니");
    	josaSet.add("다는");
    	josaSet.add("다는데");
    	josaSet.add("다니");
    	josaSet.add("다마는");
    	josaSet.add("다마다");
    	josaSet.add("다만");
    	josaSet.add("다만은");
    	josaSet.add("다며");
    	josaSet.add("다며는");
    	josaSet.add("다면");
    	josaSet.add("다면서");
    	josaSet.add("다면서도");
    	josaSet.add("다면야");
    	josaSet.add("다면은");
    	josaSet.add("다시피");
    	josaSet.add("다오");
    	josaSet.add("단");
    	josaSet.add("단다");
    	josaSet.add("담");
    	josaSet.add("답시고");
    	josaSet.add("더구나");
    	josaSet.add("더구려");
    	josaSet.add("더구먼");
    	josaSet.add("더군");
    	josaSet.add("더군요");
    	josaSet.add("더냐");
    	josaSet.add("더니");
    	josaSet.add("더니라");
    	josaSet.add("더니마는");
    	josaSet.add("더니만");
    	josaSet.add("더라");
    	josaSet.add("더라도");
    	josaSet.add("더라며는");
    	josaSet.add("더라면");
    	josaSet.add("더란");
    	josaSet.add("더면");
    	josaSet.add("던");
    	josaSet.add("던가");
    	josaSet.add("던가요");
    	josaSet.add("던걸");
    	josaSet.add("던걸요");
    	josaSet.add("던고");
    	josaSet.add("던데");
    	josaSet.add("던데다");
    	josaSet.add("던데요");
    	josaSet.add("던들");
    	josaSet.add("던지");
    	josaSet.add("데");
    	josaSet.add("데도");
    	josaSet.add("데요");
    	josaSet.add("도록");
    	josaSet.add("도록까지");
    	josaSet.add("도록까지도");
    	josaSet.add("도록까지만");
    	josaSet.add("도록까지만요");
    	josaSet.add("도록까지만은");
    	josaSet.add("되");
    	josaSet.add("든");
    	josaSet.add("든지");
    	josaSet.add("듯");
    	josaSet.add("듯이");
    	josaSet.add("디");
    	josaSet.add("라");
    	josaSet.add("라고");
    	josaSet.add("라고까지");
    	josaSet.add("라고까지는");
    	josaSet.add("라고까지도");
    	josaSet.add("라고까지만");
    	josaSet.add("라고까지만은");
    	josaSet.add("라고는");
    	josaSet.add("라고도");
    	josaSet.add("라고만");
    	josaSet.add("라고만은");
    	josaSet.add("라곤");
    	josaSet.add("라느니");
    	josaSet.add("라는");
    	josaSet.add("라는데");
    	josaSet.add("라는데도");
    	josaSet.add("라는데요");
    	josaSet.add("라니");
    	josaSet.add("라니까");
    	josaSet.add("라니까요");
    	josaSet.add("라도");
    	josaSet.add("라든지");
    	josaSet.add("라며");
    	josaSet.add("라면");
    	josaSet.add("라면서");
    	josaSet.add("라면서까지");
    	josaSet.add("라면서까지도");
    	josaSet.add("라면서도");
    	josaSet.add("라면서요");
    	josaSet.add("란");
    	josaSet.add("란다");
    	josaSet.add("란다고");
    	josaSet.add("람");
    	josaSet.add("랍니까");
    	josaSet.add("랍니다");
    	josaSet.add("랍디까");
    	josaSet.add("랍디다");
    	josaSet.add("랍시고");
    	josaSet.add("래");
    	josaSet.add("래도");
    	josaSet.add("랴");
    	josaSet.add("랴마는");
    	josaSet.add("러");
    	josaSet.add("러니");
    	josaSet.add("러니라");
    	josaSet.add("러니이까");
    	josaSet.add("러니이다");
    	josaSet.add("러만");
    	josaSet.add("러만은");
    	josaSet.add("러이까");
    	josaSet.add("러이다");
    	josaSet.add("런가");
    	josaSet.add("런들");
    	josaSet.add("려");
    	josaSet.add("려거든");
    	josaSet.add("려고");
    	josaSet.add("려고까지");
    	josaSet.add("려고까지도");
    	josaSet.add("려고까지만");
    	josaSet.add("려고까지만은");
    	josaSet.add("려고는");
    	josaSet.add("려고도");
    	josaSet.add("려고만");
    	josaSet.add("려고만은");
    	josaSet.add("려고요");
    	josaSet.add("려기에");
    	josaSet.add("려나");
    	josaSet.add("려네");
    	josaSet.add("려느냐");
    	josaSet.add("려는");
    	josaSet.add("려는가");
    	josaSet.add("려는데");
    	josaSet.add("려는데요");
    	josaSet.add("려는지");
    	josaSet.add("려니");
    	josaSet.add("려니까");
    	josaSet.add("려니와");
    	josaSet.add("려다");
    	josaSet.add("려다가");
    	josaSet.add("려다가는");
    	josaSet.add("려다가도");
    	josaSet.add("려다가요");
    	josaSet.add("려더니");
    	josaSet.add("려더니만");
    	josaSet.add("려던");
    	josaSet.add("려면");
    	josaSet.add("려면요");
    	josaSet.add("려면은");
    	josaSet.add("려무나");
    	josaSet.add("련");
    	josaSet.add("련마는");
    	josaSet.add("련만");
    	josaSet.add("렴");
    	josaSet.add("렷다");
    	josaSet.add("리");
    	josaSet.add("리까");
    	josaSet.add("리니");
    	josaSet.add("리니라");
    	josaSet.add("리다");
    	josaSet.add("리라");
    	josaSet.add("리라는");
    	josaSet.add("리란");
    	josaSet.add("리로다");
    	josaSet.add("리만치");
    	josaSet.add("리만큼");
    	josaSet.add("리요");
    	josaSet.add("리요마는");
    	josaSet.add("마");
    	josaSet.add("매");
    	josaSet.add("며");
    	josaSet.add("며는");
    	josaSet.add("면");
    	josaSet.add("면서");
    	josaSet.add("면서까지");
    	josaSet.add("면서까지도");
    	josaSet.add("면서까지만은");
    	josaSet.add("면서도");
    	josaSet.add("면서부터");
    	josaSet.add("면서부터는");
    	josaSet.add("면요");
    	josaSet.add("면은");
    	josaSet.add("므로");
    	josaSet.add("사");
    	josaSet.add("사오이다");
    	josaSet.add("사옵니까");
    	josaSet.add("사옵니다");
    	josaSet.add("사옵디까");
    	josaSet.add("사옵디다");
    	josaSet.add("사외다");
    	josaSet.add("세");
    	josaSet.add("세요");
    	josaSet.add("소");
    	josaSet.add("소서");
    	josaSet.add("소이다");
    	josaSet.add("쇠다");
    	josaSet.add("습니까");
    	josaSet.add("습니다");
    	josaSet.add("습니다마는");
    	josaSet.add("습니다만");
    	josaSet.add("습디까");
    	josaSet.add("습디다");
    	josaSet.add("습디다마는");
    	josaSet.add("습디다만");
    	josaSet.add("아");
    	josaSet.add("아다");
    	josaSet.add("아다가");
    	josaSet.add("아도");
    	josaSet.add("아라");
    	josaSet.add("아서");
    	josaSet.add("아서까지");
    	josaSet.add("아서는");
    	josaSet.add("아서도");
    	josaSet.add("아서만");
    	josaSet.add("아서요");
    	josaSet.add("아선");
    	josaSet.add("아야");
    	josaSet.add("아야만");
    	josaSet.add("아요");
    	josaSet.add("어");
    	josaSet.add("어다");
    	josaSet.add("어다가");
    	josaSet.add("어도");
    	josaSet.add("어라");
    	josaSet.add("어서");
    	josaSet.add("어서까지");
    	josaSet.add("어서는");
    	josaSet.add("어서도");
    	josaSet.add("어서만");
    	josaSet.add("어서만은");
    	josaSet.add("어선");
    	josaSet.add("어야");
    	josaSet.add("어야만");
    	josaSet.add("어야지");
    	josaSet.add("어야지만");
    	josaSet.add("어요");
    	josaSet.add("어지이다");
    	josaSet.add("언정");
    	josaSet.add("엇다");
    	josaSet.add("오");
    	josaSet.add("오리까");
    	josaSet.add("오리까마는");
    	josaSet.add("오리까만");
    	josaSet.add("오리다");
    	josaSet.add("오이다");
    	josaSet.add("올습니다");
    	josaSet.add("올습니다마는");
    	josaSet.add("올습니다만");
    	josaSet.add("올시다");
    	josaSet.add("옵나이까");
    	josaSet.add("옵나이다");
    	josaSet.add("옵니까");
    	josaSet.add("옵니다");
    	josaSet.add("옵니다만");
    	josaSet.add("옵디까");
    	josaSet.add("옵디다");
    	josaSet.add("외다");
    	josaSet.add("요");
    	josaSet.add("으나");
    	josaSet.add("으나마");
    	josaSet.add("으냐");
    	josaSet.add("으냐고");
    	josaSet.add("으니");
    	josaSet.add("으니까");
    	josaSet.add("으니까는");
    	josaSet.add("으니깐");
    	josaSet.add("으니라");
    	josaSet.add("으니만치");
    	josaSet.add("으니만큼");
    	josaSet.add("으라");
    	josaSet.add("으라고");
    	josaSet.add("으라고까지");
    	josaSet.add("으라고까지는");
    	josaSet.add("으라고까지도");
    	josaSet.add("으라고까지만은");
    	josaSet.add("으라고는");
    	josaSet.add("으라고도");
    	josaSet.add("으라고만");
    	josaSet.add("으라고만은");
    	josaSet.add("으라고요");
    	josaSet.add("으라느니");
    	josaSet.add("으라는");
    	josaSet.add("으라니");
    	josaSet.add("으라니까");
    	josaSet.add("으라든지");
    	josaSet.add("으라며");
    	josaSet.add("으라면");
    	josaSet.add("으라면서");
    	josaSet.add("으라면은");
    	josaSet.add("으란");
    	josaSet.add("으람");
    	josaSet.add("으랍니까");
    	josaSet.add("으랍니다");
    	josaSet.add("으래");
    	josaSet.add("으래서");
    	josaSet.add("으래서야");
    	josaSet.add("으래야");
    	josaSet.add("으래요");
    	josaSet.add("으랴");
    	josaSet.add("으랴마는");
    	josaSet.add("으러");
    	josaSet.add("으러까지");
    	josaSet.add("으러까지도");
    	josaSet.add("으려");
    	josaSet.add("으려거든");
    	josaSet.add("으려고");
    	josaSet.add("으려고까지");
    	josaSet.add("으려고까지는");
    	josaSet.add("으려고까지도");
    	josaSet.add("으려고까지만");
    	josaSet.add("으려고까지만은");
    	josaSet.add("으려고는");
    	josaSet.add("으려고도");
    	josaSet.add("으려고만");
    	josaSet.add("으려고만은");
    	josaSet.add("으려고요");
    	josaSet.add("으려기에");
    	josaSet.add("으려나");
    	josaSet.add("으려느냐");
    	josaSet.add("으려느냐는");
    	josaSet.add("으려는");
    	josaSet.add("으려는가");
    	josaSet.add("으려는데");
    	josaSet.add("으려는데도");
    	josaSet.add("으려는데요");
    	josaSet.add("으려는지");
    	josaSet.add("으려니");
    	josaSet.add("으려니까");
    	josaSet.add("으려니와");
    	josaSet.add("으려다");
    	josaSet.add("으려다가");
    	josaSet.add("으려다가는");
    	josaSet.add("으려다가요");
    	josaSet.add("으려다간");
    	josaSet.add("으려더니");
    	josaSet.add("으려면");
    	josaSet.add("으려면야");
    	josaSet.add("으려면은");
    	josaSet.add("으려무나");
    	josaSet.add("으려서야");
    	josaSet.add("으려오");
    	josaSet.add("으련");
    	josaSet.add("으련다");
    	josaSet.add("으련마는");
    	josaSet.add("으련만");
    	josaSet.add("으련만은");
    	josaSet.add("으렴");
    	josaSet.add("으렵니까");
    	josaSet.add("으렵니다");
    	josaSet.add("으렷다");
    	josaSet.add("으리");
    	josaSet.add("으리까");
    	josaSet.add("으리니");
    	josaSet.add("으리니라");
    	josaSet.add("으리다");
    	josaSet.add("으리라");
    	josaSet.add("으리로다");
    	josaSet.add("으리만치");
    	josaSet.add("으리만큼");
    	josaSet.add("으리요");
    	josaSet.add("으마");
    	josaSet.add("으매");
    	josaSet.add("으며");
    	josaSet.add("으면");
    	josaSet.add("으면서");
    	josaSet.add("으면서까지");
    	josaSet.add("으면서까지도");
    	josaSet.add("으면서까지만");
    	josaSet.add("으면서까지만은");
    	josaSet.add("으면서는");
    	josaSet.add("으면서도");
    	josaSet.add("으면서부터");
    	josaSet.add("으면서부터까지");
    	josaSet.add("으면서부터까지도");
    	josaSet.add("으면서부터는");
    	josaSet.add("으면서요");
    	josaSet.add("으면요");
    	josaSet.add("으면은");
    	josaSet.add("으므로");
    	josaSet.add("으세요");
    	josaSet.add("으셔요");
    	josaSet.add("으소서");
    	josaSet.add("으시어요");
    	josaSet.add("으오");
    	josaSet.add("으오리까");
    	josaSet.add("으오리다");
    	josaSet.add("으오이다");
    	josaSet.add("으옵니까");
    	josaSet.add("으옵니다");
    	josaSet.add("으옵니다만");
    	josaSet.add("으옵디까");
    	josaSet.add("으옵디다");
    	josaSet.add("으외다");
    	josaSet.add("으이");
    	josaSet.add("은");
    	josaSet.add("은가");
    	josaSet.add("은가를");
    	josaSet.add("은가에");
    	josaSet.add("은가에도");
    	josaSet.add("은가에만");
    	josaSet.add("은가요");
    	josaSet.add("은걸");
    	josaSet.add("은걸요");
    	josaSet.add("은고");
    	josaSet.add("은다고");
    	josaSet.add("은다고까지");
    	josaSet.add("은다고까지도");
    	josaSet.add("은다고는");
    	josaSet.add("은다는");
    	josaSet.add("은다는데");
    	josaSet.add("은다니");
    	josaSet.add("은다니까");
    	josaSet.add("은다든지");
    	josaSet.add("은다마는");
    	josaSet.add("은다면");
    	josaSet.add("은다면서");
    	josaSet.add("은다면서도");
    	josaSet.add("은다면요");
    	josaSet.add("은다면은");
    	josaSet.add("은단다");
    	josaSet.add("은담");
    	josaSet.add("은답니까");
    	josaSet.add("은답니다");
    	josaSet.add("은답디까");
    	josaSet.add("은답디다");
    	josaSet.add("은답시고");
    	josaSet.add("은대");
    	josaSet.add("은대서");
    	josaSet.add("은대서야");
    	josaSet.add("은대야");
    	josaSet.add("은대요");
    	josaSet.add("은데");
    	josaSet.add("은데는");
    	josaSet.add("은데다");
    	josaSet.add("은데도");
    	josaSet.add("은데도요");
    	josaSet.add("은데서");
    	josaSet.add("은들");
    	josaSet.add("은만큼");
    	josaSet.add("은만큼도");
    	josaSet.add("은만큼만은");
    	josaSet.add("은만큼은");
    	josaSet.add("은바");
    	josaSet.add("은즉");
    	josaSet.add("은즉슨");
    	josaSet.add("은지");
    	josaSet.add("은지가");
    	josaSet.add("은지고");
    	josaSet.add("은지는");
    	josaSet.add("은지도");
    	josaSet.add("은지라");
    	josaSet.add("은지라도");
    	josaSet.add("은지를");
    	josaSet.add("은지만");
    	josaSet.add("은지만은");
    	josaSet.add("은지요");
    	josaSet.add("을");
    	josaSet.add("을거나");
    	josaSet.add("을거냐");
    	josaSet.add("을거다");
    	josaSet.add("을거야");
    	josaSet.add("을거지요");
    	josaSet.add("을걸");
    	josaSet.add("을까");
    	josaSet.add("을까마는");
    	josaSet.add("을까봐");
    	josaSet.add("을까요");
    	josaSet.add("을께");
    	josaSet.add("을께요");
    	josaSet.add("을꼬");
    	josaSet.add("을는지");
    	josaSet.add("을는지요");
    	josaSet.add("을라");
    	josaSet.add("을라고");
    	josaSet.add("을라고까지");
    	josaSet.add("을라고까지도");
    	josaSet.add("을라고까지만");
    	josaSet.add("을라고는");
    	josaSet.add("을라고도");
    	josaSet.add("을라고만");
    	josaSet.add("을라고만은");
    	josaSet.add("을라고요");
    	josaSet.add("을라요");
    	josaSet.add("을라치면");
    	josaSet.add("을락");
    	josaSet.add("을래");
    	josaSet.add("을래도");
    	josaSet.add("을래요");
    	josaSet.add("을러니");
    	josaSet.add("을러라");
    	josaSet.add("을런가");
    	josaSet.add("을런고");
    	josaSet.add("을레");
    	josaSet.add("을레라");
    	josaSet.add("을만한");
    	josaSet.add("을망정");
    	josaSet.add("을밖에");
    	josaSet.add("을밖에요");
    	josaSet.add("을뿐더러");
    	josaSet.add("을새");
    	josaSet.add("을세라");
    	josaSet.add("을세말이지");
    	josaSet.add("을소냐");
    	josaSet.add("을수록");
    	josaSet.add("을쏘냐");
    	josaSet.add("을이만큼");
    	josaSet.add("을작이면");
    	josaSet.add("을지");
    	josaSet.add("을지가");
    	josaSet.add("을지나");
    	josaSet.add("을지니");
    	josaSet.add("을지니라");
    	josaSet.add("을지도");
    	josaSet.add("을지라");
    	josaSet.add("을지라도");
    	josaSet.add("을지어다");
    	josaSet.add("을지언정");
    	josaSet.add("을지요");
    	josaSet.add("을진대");
    	josaSet.add("을진댄");
    	josaSet.add("을진저");
    	josaSet.add("을테다");
    	josaSet.add("을텐데");
    	josaSet.add("음");
    	josaSet.add("음세");
    	josaSet.add("음에도");
    	josaSet.add("음에랴");
    	josaSet.add("읍쇼");
    	josaSet.add("읍시다");
    	josaSet.add("읍시다요");
    	josaSet.add("읍시오");
    	josaSet.add("자");
    	josaSet.add("자고");
    	josaSet.add("자고까지");
    	josaSet.add("자고까지는");
    	josaSet.add("자고까지라도");
    	josaSet.add("자고는");
    	josaSet.add("자고도");
    	josaSet.add("자고만");
    	josaSet.add("자고만은");
    	josaSet.add("자꾸나");
    	josaSet.add("자는");
    	josaSet.add("자마자");
    	josaSet.add("자면");
    	josaSet.add("자면요");
    	josaSet.add("잔");
    	josaSet.add("잘");
    	josaSet.add("지");
    	josaSet.add("지는");
    	josaSet.add("지도");
    	josaSet.add("지를");
    	josaSet.add("지마는");
    	josaSet.add("지만");
    	josaSet.add("지요");
    	josaSet.add("진");
    	josaSet.add("질");
    	josaSet.add("가");
    	josaSet.add("같이");
    	josaSet.add("같이나");
    	josaSet.add("같이는");
    	josaSet.add("같이는야");
    	josaSet.add("같이는커녕");
    	josaSet.add("같이도");
    	josaSet.add("같이만");
    	josaSet.add("같인");
    	josaSet.add("고");
    	josaSet.add("과");
    	josaSet.add("과는");
    	josaSet.add("과는커녕");
    	josaSet.add("과도");
    	josaSet.add("과를");
    	josaSet.add("과만");
    	josaSet.add("과만은");
    	josaSet.add("과의");
    	josaSet.add("까지");
    	josaSet.add("까지가");
    	josaSet.add("까지나");
    	josaSet.add("까지나마");
    	josaSet.add("까지는");
    	josaSet.add("까지는야");
    	josaSet.add("까지는커녕");
    	josaSet.add("까지도");
    	josaSet.add("까지든지");
    	josaSet.add("까지라고");
    	josaSet.add("까지라고는");
    	josaSet.add("까지라고만은");
    	josaSet.add("까지라도");
    	josaSet.add("까지로");
    	josaSet.add("까지로나");
    	josaSet.add("까지로나마");
    	josaSet.add("까지로는");
    	josaSet.add("까지로는야");
    	josaSet.add("까지로는커녕");
    	josaSet.add("까지로도");
    	josaSet.add("까지로든");
    	josaSet.add("까지로든지");
    	josaSet.add("까지로라서");
    	josaSet.add("까지로라야");
    	josaSet.add("까지로만");
    	josaSet.add("까지로만은");
    	josaSet.add("까지로서");
    	josaSet.add("까지로써");
    	josaSet.add("까지를");
    	josaSet.add("까지만");
    	josaSet.add("까지만은");
    	josaSet.add("까지만이라도");
    	josaSet.add("까지야");
    	josaSet.add("까지야말로");
    	josaSet.add("까지에");
    	josaSet.add("까지와");
    	josaSet.add("까지의");
    	josaSet.add("까지조차");
    	josaSet.add("까지조차도");
    	josaSet.add("까진");
    	josaSet.add("께옵서");
    	josaSet.add("께옵서는");
    	josaSet.add("께옵서는야");
    	josaSet.add("께옵서는커녕");
    	josaSet.add("께옵서도");
    	josaSet.add("께옵서만");
    	josaSet.add("께옵서만은");
    	josaSet.add("께옵서만이");
    	josaSet.add("께옵선");
    	josaSet.add("나");
    	josaSet.add("나마");
    	josaSet.add("는");
    	josaSet.add("는야");
    	josaSet.add("는커녕");
    	josaSet.add("니");
    	josaSet.add("다");
    	josaSet.add("다가");
    	josaSet.add("다가는");
    	josaSet.add("다가도");
    	josaSet.add("다간");
    	josaSet.add("대로");
    	josaSet.add("대로가");
    	josaSet.add("대로는");
    	josaSet.add("대로의");
    	josaSet.add("더러");
    	josaSet.add("더러는");
    	josaSet.add("더러만은");
    	josaSet.add("도");
    	josaSet.add("든");
    	josaSet.add("든지");
    	josaSet.add("라");
    	josaSet.add("라고");
    	josaSet.add("라고까지");
    	josaSet.add("라고까지는");
    	josaSet.add("라고는");
    	josaSet.add("라고만은");
    	josaSet.add("라곤");
    	josaSet.add("라도");
    	josaSet.add("라든지");
    	josaSet.add("라서");
    	josaSet.add("라야");
    	josaSet.add("라야만");
    	josaSet.add("라오");
    	josaSet.add("라지");
    	josaSet.add("라지요");
    	josaSet.add("랑");
    	josaSet.add("랑은");
    	josaSet.add("로고");
    	josaSet.add("로구나");
    	josaSet.add("로구려");
    	josaSet.add("로구먼");
    	josaSet.add("로군");
    	josaSet.add("로군요");
    	josaSet.add("로다");
    	josaSet.add("로되");
    	josaSet.add("로세");
    	josaSet.add("를");
    	josaSet.add("마다");
    	josaSet.add("마다라도");
    	josaSet.add("마다를");
    	josaSet.add("마다에게");
    	josaSet.add("마다의");
    	josaSet.add("마따나");
    	josaSet.add("마저");
    	josaSet.add("마저나마라도");
    	josaSet.add("마저도");
    	josaSet.add("마저라도");
    	josaSet.add("마저야");
    	josaSet.add("만");
    	josaSet.add("만도");
    	josaSet.add("만에");
    	josaSet.add("만으로");
    	josaSet.add("만으로는");
    	josaSet.add("만으로도");
    	josaSet.add("만으로라도");
    	josaSet.add("만으로써");
    	josaSet.add("만으론");
    	josaSet.add("만은");
    	josaSet.add("만을");
    	josaSet.add("만의");
    	josaSet.add("만이");
    	josaSet.add("만이라도");
    	josaSet.add("만치");
    	josaSet.add("만큼");
    	josaSet.add("만큼도");
    	josaSet.add("만큼만");
    	josaSet.add("만큼씩");
    	josaSet.add("만큼은");
    	josaSet.add("만큼의");
    	josaSet.add("만큼이나");
    	josaSet.add("만큼이라도");
    	josaSet.add("만큼이야");
    	josaSet.add("말고");
    	josaSet.add("말고는");
    	josaSet.add("말고도");
    	josaSet.add("며");
    	josaSet.add("밖에");
    	josaSet.add("밖에는");
    	josaSet.add("밖에도");
    	josaSet.add("밖엔");
    	josaSet.add("보고");
    	josaSet.add("보고는");
    	josaSet.add("보고도");
    	josaSet.add("보고만");
    	josaSet.add("보고만은");
    	josaSet.add("보고만이라도");
    	josaSet.add("보곤");
    	josaSet.add("보다");
    	josaSet.add("보다는");
    	josaSet.add("보다는야");
    	josaSet.add("보다도");
    	josaSet.add("보다만");
    	josaSet.add("보다야");
    	josaSet.add("보단");
    	josaSet.add("부터");
    	josaSet.add("부터가");
    	josaSet.add("부터나마");
    	josaSet.add("부터는");
    	josaSet.add("부터도");
    	josaSet.add("부터라도");
    	josaSet.add("부터를");
    	josaSet.add("부터만");
    	josaSet.add("부터만은");
    	josaSet.add("부터서는");
    	josaSet.add("부터야말로");
    	josaSet.add("부터의");
    	josaSet.add("부턴");
    	josaSet.add("아");
    	josaSet.add("야");
    	josaSet.add("야말로");
    	josaSet.add("에");
    	josaSet.add("에게");
    	josaSet.add("에게가");
    	josaSet.add("에게까지");
    	josaSet.add("에게까지는");
    	josaSet.add("에게까지는커녕");
    	josaSet.add("에게까지도");
    	josaSet.add("에게까지만");
    	josaSet.add("에게까지만은");
    	josaSet.add("에게나");
    	josaSet.add("에게는");
    	josaSet.add("에게는커녕");
    	josaSet.add("에게다");
    	josaSet.add("에게도");
    	josaSet.add("에게든");
    	josaSet.add("에게든지");
    	josaSet.add("에게라도");
    	josaSet.add("에게로");
    	josaSet.add("에게로는");
    	josaSet.add("에게마다");
    	josaSet.add("에게만");
    	josaSet.add("에게며");
    	josaSet.add("에게보다");
    	josaSet.add("에게보다는");
    	josaSet.add("에게부터");
    	josaSet.add("에게서");
    	josaSet.add("에게서가");
    	josaSet.add("에게서까지");
    	josaSet.add("에게서나");
    	josaSet.add("에게서는");
    	josaSet.add("에게서도");
    	josaSet.add("에게서든지");
    	josaSet.add("에게서라도");
    	josaSet.add("에게서만");
    	josaSet.add("에게서보다");
    	josaSet.add("에게서부터");
    	josaSet.add("에게서야");
    	josaSet.add("에게서와");
    	josaSet.add("에게서의");
    	josaSet.add("에게서처럼");
    	josaSet.add("에게선");
    	josaSet.add("에게야");
    	josaSet.add("에게와");
    	josaSet.add("에게의");
    	josaSet.add("에게처럼");
    	josaSet.add("에게하고");
    	josaSet.add("에게하며");
    	josaSet.add("에겐");
    	josaSet.add("에까지");
    	josaSet.add("에까지는");
    	josaSet.add("에까지도");
    	josaSet.add("에까지든지");
    	josaSet.add("에까지라도");
    	josaSet.add("에까지만");
    	josaSet.add("에까지만은");
    	josaSet.add("에까진");
    	josaSet.add("에나");
    	josaSet.add("에는");
    	josaSet.add("에다");
    	josaSet.add("에다가");
    	josaSet.add("에다가는");
    	josaSet.add("에다간");
    	josaSet.add("에도");
    	josaSet.add("에든");
    	josaSet.add("에든지");
    	josaSet.add("에라도");
    	josaSet.add("에로");
    	josaSet.add("에로의");
    	josaSet.add("에를");
    	josaSet.add("에만");
    	josaSet.add("에만은");
    	josaSet.add("에부터");
    	josaSet.add("에서");
    	josaSet.add("에서가");
    	josaSet.add("에서까지");
    	josaSet.add("에서까지도");
    	josaSet.add("에서나");
    	josaSet.add("에서나마");
    	josaSet.add("에서는");
    	josaSet.add("에서도");
    	josaSet.add("에서든지");
    	josaSet.add("에서라도");
    	josaSet.add("에서만");
    	josaSet.add("에서만도");
    	josaSet.add("에서만이");
    	josaSet.add("에서만큼");
    	josaSet.add("에서만큼은");
    	josaSet.add("에서보다");
    	josaSet.add("에서부터");
    	josaSet.add("에서부터는");
    	josaSet.add("에서부터도");
    	josaSet.add("에서부터라도");
    	josaSet.add("에서부터만");
    	josaSet.add("에서부터만은");
    	josaSet.add("에서야");
    	josaSet.add("에서와");
    	josaSet.add("에서와는");
    	josaSet.add("에서와의");
    	josaSet.add("에서의");
    	josaSet.add("에서조차");
    	josaSet.add("에서처럼");
    	josaSet.add("에선");
    	josaSet.add("에야");
    	josaSet.add("에의");
    	josaSet.add("에조차도");
    	josaSet.add("에하며");
    	josaSet.add("엔");
    	josaSet.add("엔들");
    	josaSet.add("엘");
    	josaSet.add("엘랑");
    	josaSet.add("여");
    	josaSet.add("와");
    	josaSet.add("와는");
    	josaSet.add("와도");
    	josaSet.add("와라도");
    	josaSet.add("와를");
    	josaSet.add("와만");
    	josaSet.add("와만은");
    	josaSet.add("와에만");
    	josaSet.add("와의");
    	josaSet.add("와처럼");
    	josaSet.add("와한테");
    	josaSet.add("요");
    	josaSet.add("으로");
    	josaSet.add("으로가");
    	josaSet.add("으로까지");
    	josaSet.add("으로까지만은");
    	josaSet.add("으로나");
    	josaSet.add("으로나든지");
    	josaSet.add("으로는");
    	josaSet.add("으로도");
    	josaSet.add("으로든지");
    	josaSet.add("으로라도");
    	josaSet.add("으로랑");
    	josaSet.add("으로만");
    	josaSet.add("으로만은");
    	josaSet.add("으로부터");
    	josaSet.add("으로부터는");
    	josaSet.add("으로부터는커녕");
    	josaSet.add("으로부터도");
    	josaSet.add("으로부터만");
    	josaSet.add("으로부터만은");
    	josaSet.add("으로부터서는");
    	josaSet.add("으로부터서도");
    	josaSet.add("으로부터서만");
    	josaSet.add("으로부터의");
    	josaSet.add("으로서");
    	josaSet.add("으로서가");
    	josaSet.add("으로서나");
    	josaSet.add("으로서는");
    	josaSet.add("으로서도");
    	josaSet.add("으로서든지");
    	josaSet.add("으로서라도");
    	josaSet.add("으로서만");
    	josaSet.add("으로서만도");
    	josaSet.add("으로서만은");
    	josaSet.add("으로서야");
    	josaSet.add("으로서의");
    	josaSet.add("으로선");
    	josaSet.add("으로써");
    	josaSet.add("으로써나");
    	josaSet.add("으로써는");
    	josaSet.add("으로써라도");
    	josaSet.add("으로써만");
    	josaSet.add("으로써야");
    	josaSet.add("으로야");
    	josaSet.add("으로의");
    	josaSet.add("으론");
    	josaSet.add("은");
    	josaSet.add("은커녕");
    	josaSet.add("을");
    	josaSet.add("의");
    	josaSet.add("이");
    	josaSet.add("이고");
    	josaSet.add("이나");
    	josaSet.add("이나마");
    	josaSet.add("이니");
    	josaSet.add("이다");
    	josaSet.add("이든");
    	josaSet.add("이든지");
    	josaSet.add("이라");
    	josaSet.add("이라고");
    	josaSet.add("이라고는");
    	josaSet.add("이라고도");
    	josaSet.add("이라고만은");
    	josaSet.add("이라곤");
    	josaSet.add("이라는");
    	josaSet.add("이라도");
    	josaSet.add("이라든지");
    	josaSet.add("이라서");
    	josaSet.add("이라야");
    	josaSet.add("이라야만");
    	josaSet.add("이랑");
    	josaSet.add("이랑은");
    	josaSet.add("이며");
    	josaSet.add("이며에게");
    	josaSet.add("이며조차도");
    	josaSet.add("이야");
    	josaSet.add("이야말로");
    	josaSet.add("이여");
    	josaSet.add("인들");
    	josaSet.add("인즉");
    	josaSet.add("인즉슨");
    	josaSet.add("일랑");
    	josaSet.add("일랑은");
    	josaSet.add("조차");
    	josaSet.add("조차가");
    	josaSet.add("조차도");
    	josaSet.add("조차를");
    	josaSet.add("조차의");
    	josaSet.add("처럼");
    	josaSet.add("처럼과");
    	josaSet.add("처럼도");
    	josaSet.add("처럼만");
    	josaSet.add("처럼만은");
    	josaSet.add("처럼은");
    	josaSet.add("처럼이라도");
    	josaSet.add("처럼이야");
    	josaSet.add("치고");
    	josaSet.add("치고는");
    	josaSet.add("커녕");
    	josaSet.add("커녕은");
    	josaSet.add("커니와");
    	josaSet.add("토록");
    	josaSet.add("하고");
    	josaSet.add("하고가");
    	josaSet.add("하고는");
    	josaSet.add("하고는커녕");
    	josaSet.add("하고도");
    	josaSet.add("하고라도");
    	josaSet.add("하고마저");
    	josaSet.add("하고만");
    	josaSet.add("하고만은");
    	josaSet.add("하고야");
    	josaSet.add("하고에게");
    	josaSet.add("하고의");
    	josaSet.add("하고조차");
    	josaSet.add("하고조차도");
    	josaSet.add("하곤");
    	//중요 키워드 로
    	importantkey = new HashMap<String, Double> ();
    	Path[] targetPathArr   = null;
    	FileSystem fs = null;
    	if(keyword == true)
    	{
	    	try
	    	{
	    		targetPathArr = DistributedCache.getLocalCacheFiles(conf);//Get All Term
	    		if(targetPathArr == null)
	    		{
	    			logger.error(this.toString() + "-DistributedCache Path is null");
	    			System.exit(1);
	    			return;
	    		}
	    		else
	    		{
	    			logger.info(this.toString() +"DistributedCache ok");
	    		}
	            fs = FileSystem.getLocal(conf);
	            String m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
	            for(Path p: targetPathArr)
	            {
	            	System.out.println(p.getName());
	            	if(p.getName().equals("keyword.txt") == true)
	            	{
		            	FSDataInputStream fin = fs.open(p);
		                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
		                String line;
		                while((line = br.readLine())!= null)
		    			{
		                	importantkey.put(line, 1.0);
		    			}
		                br.close();
		                fin.close();
	            	}
	            }
	            fs.close();
	    	}
	    	catch(Exception e)
	    	{
	    		logger.error(e.toString());
	    	}
    	}
    }
    public ArirangAnalyzerHandler(boolean use_keyword )
    {
    	//중요 키워드 로
    	importantkey = new HashMap<String, Double> ();
    	this.use_keyword = use_keyword;
    	if(use_keyword == true)
    	{
    		importantkey.put("건강기능식품",1.0);
    		importantkey.put("과대광고",1.0);
    		importantkey.put("단백질",1.0);
    		importantkey.put("무기질",1.0);
    		importantkey.put("미네랄",1.0);
    		importantkey.put("발기부전치료제",1.0);
    		importantkey.put("비타민",1.0);
    		importantkey.put("비타민A",1.0);
    		importantkey.put("비타민C",1.0);
    		importantkey.put("비타민D",1.0);
    		importantkey.put("비타민E",1.0);
    		importantkey.put("색소",1.0);
    		importantkey.put("섭취량",1.0);
    		importantkey.put("성분",1.0);
    		importantkey.put("성분들",1.0);
    		importantkey.put("셀레늄",1.0);
    		importantkey.put("스테로이드",1.0);
    		importantkey.put("식이섬유",1.0);
    		importantkey.put("식품첨가물",1.0);
    		importantkey.put("아연",1.0);
    		importantkey.put("영양성분",1.0);
    		importantkey.put("영양소",1.0);
    		importantkey.put("오메가3",1.0);
    		importantkey.put("요오드",1.0);
    		importantkey.put("원료",1.0);
    		importantkey.put("유산균",1.0);
    		importantkey.put("유해물질",1.0);
    		importantkey.put("인공향료",1.0);
    		importantkey.put("주성분",1.0);
    		importantkey.put("주요성분",1.0);
    		importantkey.put("주원료",1.0);
    		importantkey.put("중금속",1.0);
    		importantkey.put("천연",1.0);
    		importantkey.put("천연성분",1.0);
    		importantkey.put("첨가물",1.0);
    		importantkey.put("추출물",1.0);
    		importantkey.put("칼륨",1.0);
    		importantkey.put("칼슘",1.0);
    		importantkey.put("콜라겐",1.0);
    		importantkey.put("콜레스테롤",1.0);
    		importantkey.put("프로폴리스",1.0);
    		importantkey.put("합성착색료",1.0);
    		importantkey.put("항산화",1.0);
    		importantkey.put("향료",1.0);
    		importantkey.put("홍삼",1.0);
    		importantkey.put("히알루론산",1.0);
    		importantkey.put("가공식품",1.0);
    		importantkey.put("간식",1.0);
    		importantkey.put("급식",1.0);
    		importantkey.put("기호식품",1.0);
    		importantkey.put("라면",1.0);
    		importantkey.put("먹거리",1.0);
    		importantkey.put("불량식품",1.0);
    		importantkey.put("비만",1.0);
    		importantkey.put("색소",1.0);
    		importantkey.put("설탕",1.0);
    		importantkey.put("식생활",1.0);
    		importantkey.put("식습관",1.0);
    		importantkey.put("식품안전",1.0);
    		importantkey.put("아이스크림",1.0);
    		importantkey.put("영양성분",1.0);
    		importantkey.put("영양소",1.0);
    		importantkey.put("음료",1.0);
    		importantkey.put("초콜릿",1.0);
    		importantkey.put("카페인",1.0);
    		importantkey.put("칼로리",1.0);
    		importantkey.put("학교급식",1.0);
    		importantkey.put("합성착색료",1.0);
    		importantkey.put("도시락",1.0);
    		importantkey.put("불량식품",1.0);
    		importantkey.put("소화불량",1.0);
    		importantkey.put("식중독",1.0);
    		importantkey.put("식중독균",1.0);
    		importantkey.put("식품",1.0);
    		importantkey.put("음료",1.0);
    		importantkey.put("음식",1.0);
    		importantkey.put("음식물",1.0);
    		importantkey.put("음식점",1.0);
    		importantkey.put("의사",1.0);
    		importantkey.put("의원",1.0);
    		importantkey.put("학교급식",1.0);
    		importantkey.put("GMO",1.0);
    		importantkey.put("가공식품",1.0);
    		importantkey.put("건강기능식품",1.0);
    		importantkey.put("농산물",1.0);
    		importantkey.put("미생물",1.0);
    		importantkey.put("수입금지",1.0);
    		importantkey.put("수입식품",1.0);
    		importantkey.put("식물",1.0);
    		importantkey.put("식용",1.0);
    		importantkey.put("식품",1.0);
    		importantkey.put("식품안전",1.0);
    		importantkey.put("식품위생법",1.0);
    		importantkey.put("식품제조",1.0);
    		importantkey.put("옥수수",1.0);
    		importantkey.put("원료",1.0);
    		importantkey.put("주원료",1.0);
    		importantkey.put("MSG",1.0);
    		importantkey.put("가공식품",1.0);
    		importantkey.put("간식",1.0);
    		importantkey.put("감기",1.0);
    		importantkey.put("건강기능식품",1.0);
    		importantkey.put("건강식품",1.0);
    		importantkey.put("견과류",1.0);
    		importantkey.put("과대광고",1.0);
    		importantkey.put("과일",1.0);
    		importantkey.put("글루타민산나트륨",1.0);
    		importantkey.put("글리세린",1.0);
    		importantkey.put("급식",1.0);
    		importantkey.put("기호식품",1.0);
    		importantkey.put("김치",1.0);
    		importantkey.put("나트륨",1.0);
    		importantkey.put("노화방지",1.0);
    		importantkey.put("단맛",1.0);
    		importantkey.put("단백질",1.0);
    		importantkey.put("당근",1.0);
    		importantkey.put("당류",1.0);
    		importantkey.put("도시락",1.0);
    		importantkey.put("돼지고기",1.0);
    		importantkey.put("된장",1.0);
    		importantkey.put("라면",1.0);
    		importantkey.put("루틴",1.0);
    		importantkey.put("마늘",1.0);
    		importantkey.put("망간",1.0);
    		importantkey.put("먹거리",1.0);
    		importantkey.put("면역력",1.0);
    		importantkey.put("면역력증진",1.0);
    		importantkey.put("몸매",1.0);
    		importantkey.put("무기질",1.0);
    		importantkey.put("미네랄",1.0);
    		importantkey.put("반찬",1.0);
    		importantkey.put("발효",1.0);
    		importantkey.put("버섯",1.0);
    		importantkey.put("보충제",1.0);
    		importantkey.put("불량식품",1.0);
    		importantkey.put("불소",1.0);
    		importantkey.put("비만",1.0);
    		importantkey.put("비타민",1.0);
    		importantkey.put("비타민A",1.0);
    		importantkey.put("비타민C",1.0);
    		importantkey.put("비타민D",1.0);
    		importantkey.put("비타민E",1.0);
    		importantkey.put("사과",1.0);
    		importantkey.put("사카린",1.0);
    		importantkey.put("사탕수수",1.0);
    		importantkey.put("사포닌",1.0);
    		importantkey.put("설탕",1.0);
    		importantkey.put("섭취량",1.0);
    		importantkey.put("성분",1.0);
    		importantkey.put("성분들",1.0);
    		importantkey.put("셀레늄",1.0);
    		importantkey.put("수박",1.0);
    		importantkey.put("수분",1.0);
    		importantkey.put("수입식품",1.0);
    		importantkey.put("식단",1.0);
    		importantkey.put("식당",1.0);
    		importantkey.put("식물성",1.0);
    		importantkey.put("식사",1.0);
    		importantkey.put("식생활",1.0);
    		importantkey.put("식습관",1.0);
    		importantkey.put("식이섬유",1.0);
    		importantkey.put("식재료",1.0);
    		importantkey.put("식중독",1.0);
    		importantkey.put("식중독균",1.0);
    		importantkey.put("식품",1.0);
    		importantkey.put("식품안전",1.0);
    		importantkey.put("식품위생법",1.0);
    		importantkey.put("식품제조",1.0);
    		importantkey.put("식후",1.0);
    		importantkey.put("아데노신",1.0);
    		importantkey.put("아미노산",1.0);
    		importantkey.put("아연",1.0);
    		importantkey.put("아이스크림",1.0);
    		importantkey.put("야채",1.0);
    		importantkey.put("양파",1.0);
    		importantkey.put("어패류",1.0);
    		importantkey.put("영양분",1.0);
    		importantkey.put("영양성분",1.0);
    		importantkey.put("영양소",1.0);
    		importantkey.put("오리고기",1.0);
    		importantkey.put("오메가3",1.0);
    		importantkey.put("오일",1.0);
    		importantkey.put("외식",1.0);
    		importantkey.put("요리",1.0);
    		importantkey.put("요오드",1.0);
    		importantkey.put("우유",1.0);
    		importantkey.put("원료",1.0);
    		importantkey.put("유산균",1.0);
    		importantkey.put("유제품",1.0);
    		importantkey.put("유해물질",1.0);
    		importantkey.put("유효성분",1.0);
    		importantkey.put("음료",1.0);
    		importantkey.put("음식",1.0);
    		importantkey.put("음식물",1.0);
    		importantkey.put("음식점",1.0);
    		importantkey.put("이유식",1.0);
    		importantkey.put("인슐린",1.0);
    		importantkey.put("젓갈",1.0);
    		importantkey.put("조리",1.0);
    		importantkey.put("주성분",1.0);
    		importantkey.put("주요성분",1.0);
    		importantkey.put("주원료",1.0);
    		importantkey.put("지방",1.0);
    		importantkey.put("채소",1.0);
    		importantkey.put("천연",1.0);
    		importantkey.put("체중",1.0);
    		importantkey.put("초콜릿",1.0);
    		importantkey.put("축산물",1.0);
    		importantkey.put("치즈",1.0);
    		importantkey.put("카페인",1.0);
    		importantkey.put("칼로리",1.0);
    		importantkey.put("칼륨",1.0);
    		importantkey.put("커피",1.0);
    		importantkey.put("콜레스테롤",1.0);
    		importantkey.put("탄수화물",1.0);
    		importantkey.put("토마토",1.0);
    		importantkey.put("트랜스지방",1.0);
    		importantkey.put("판매금지",1.0);
    		importantkey.put("학교급식",1.0);
    		importantkey.put("항산화",1.0);
    		importantkey.put("홍삼",1.0);
    		importantkey.put("AI",1.0);
    		importantkey.put("BHA",1.0);
    		importantkey.put("BHT",1.0);
    		importantkey.put("GMO",1.0);
    		importantkey.put("MSG",1.0);
    		importantkey.put("Pb",1.0);
    		importantkey.put("TEA",1.0);
    		importantkey.put("가공식품",1.0);
    		importantkey.put("간식",1.0);
    		importantkey.put("건강기능식품",1.0);
    		importantkey.put("건강식품",1.0);
    		importantkey.put("견과류",1.0);
    		importantkey.put("계면활성제",1.0);
    		importantkey.put("고기",1.0);
    		importantkey.put("고사리",1.0);
    		importantkey.put("고카페인",1.0);
    		importantkey.put("곰팡이",1.0);
    		importantkey.put("과대광고",1.0);
    		importantkey.put("과일",1.0);
    		importantkey.put("글루타민산나트륨",1.0);
    		importantkey.put("클리세린",1.0);
    		importantkey.put("급식",1.0);
    		importantkey.put("기생충",1.0);
    		importantkey.put("기호식품",1.0);
    		importantkey.put("김치",1.0);
    		importantkey.put("나트륨",1.0);
    		importantkey.put("노로바이러스",1.0);
    		importantkey.put("녹용",1.0);
    		importantkey.put("녹차",1.0);
    		importantkey.put("농산물",1.0);
    		importantkey.put("농약",1.0);
    		importantkey.put("니켈",1.0);
    		importantkey.put("니코틴",1.0);
    		importantkey.put("다이옥신",1.0);
    		importantkey.put("단맛",1.0);
    		importantkey.put("단백질",1.0);
    		importantkey.put("담배",1.0);
    		importantkey.put("당근",1.0);
    		importantkey.put("당류",1.0);
    		importantkey.put("도시락",1.0);
    		importantkey.put("독성",1.0);
    		importantkey.put("독성물질",1.0);
    		importantkey.put("독성성분",1.0);
    		importantkey.put("돼지고기",1.0);
    		importantkey.put("된장",1.0);
    		importantkey.put("디메치콘",1.0);
    		importantkey.put("디소듐이디티에이",1.0);
    		importantkey.put("디에틸핵실프탈레이트",1.0);
    		importantkey.put("라면",1.0);
    		importantkey.put("루틴",1.0);
    		importantkey.put("리스테리아",1.0);
    		importantkey.put("마늘",1.0);
    		importantkey.put("망간",1.0);
    		importantkey.put("맥주",1.0);
    		importantkey.put("먹거리",1.0);
    		importantkey.put("멜라민",1.0);
    		importantkey.put("면역력",1.0);
    		importantkey.put("면역력증진",1.0);
    		importantkey.put("몸매",1.0);
    		importantkey.put("무기질",1.0);
    		importantkey.put("물질",1.0);
    		importantkey.put("미네랄",1.0);
    		importantkey.put("미네랄오일",1.0);
    		importantkey.put("미생물",1.0);
    		importantkey.put("미세먼지",1.0);
    		importantkey.put("바이오제닉아민",1.0);
    		importantkey.put("박테리아",1.0);
    		importantkey.put("반찬",1.0);
    		importantkey.put("발암물질",1.0);
    		importantkey.put("발암성분",1.0);
    		importantkey.put("발효",1.0);
    		importantkey.put("방부제",1.0);
    		importantkey.put("방사능",1.0);
    		importantkey.put("방사성",1.0);
    		importantkey.put("버섯",1.0);
    		importantkey.put("벤젠",1.0);
    		importantkey.put("벤조페논",1.0);
    		importantkey.put("벤조피렌",1.0);
    		importantkey.put("병원성대장균",1.0);
    		importantkey.put("봄나물",1.0);
    		importantkey.put("부틸파라벤",1.0);
    		importantkey.put("부틸렌클라이콜",1.0);
    		importantkey.put("불량식품",1.0);
    		importantkey.put("불소",1.0);
    		importantkey.put("비만",1.0);
    		importantkey.put("비소",1.0);
    		importantkey.put("비스페놀A",1.0);
    		importantkey.put("비타민",1.0);
    		importantkey.put("비타민A",1.0);
    		importantkey.put("비타민C",1.0);
    		importantkey.put("비타민D",1.0);
    		importantkey.put("비타민E",1.0);
    		importantkey.put("비펜스린",1.0);
    		importantkey.put("사과",1.0);
    		importantkey.put("사카린",1.0);
    		importantkey.put("사탕수수",1.0);
    		importantkey.put("사포닌",1.0);
    		importantkey.put("살리실산",1.0);
    		importantkey.put("살모렐라",1.0);
    		importantkey.put("색소",1.0);
    		importantkey.put("설탕",1.0);
    		importantkey.put("설페이트",1.0);
    		importantkey.put("섭취량",1.0);
    		importantkey.put("성분",1.0);
    		importantkey.put("성분들",1.0);
    		importantkey.put("세트리모늄브로마이드",1.0);
    		importantkey.put("셀레늄",1.0);
    		importantkey.put("소르빈산",1.0);
    		importantkey.put("소식",1.0);
    		importantkey.put("수박",1.0);
    		importantkey.put("수분",1.0);
    		importantkey.put("수산물",1.0);
    		importantkey.put("수입금지",1.0);
    		importantkey.put("수입식품",1.0);
    		importantkey.put("수지",1.0);
    		importantkey.put("스테로이드",1.0);
    		importantkey.put("스테아레이트",1.0);
    		importantkey.put("시프로플록사신",1.0);
    		importantkey.put("식단",1.0);
    		importantkey.put("식당",1.0);
    		importantkey.put("식물",1.0);
    		importantkey.put("식물성",1.0);
    		importantkey.put("식사",1.0);
    		importantkey.put("식생활",1.0);
    		importantkey.put("식습관",1.0);
    		importantkey.put("식용",1.0);
    		importantkey.put("식이섬유",1.0);
    		importantkey.put("식재료",1.0);
    		importantkey.put("식중독",1.0);
    		importantkey.put("식중독균",1.0);
    		importantkey.put("식초",1.0);
    		importantkey.put("식탁",1.0);
    		importantkey.put("식품",1.0);
    		importantkey.put("식품안전",1.0);
    		importantkey.put("식품위생법",1.0);
    		importantkey.put("식품제조",1.0);
    		importantkey.put("식품첨가물",1.0);
    		importantkey.put("식후",1.0);
    		importantkey.put("실리콘",1.0);
    		importantkey.put("아데노신",1.0);
    		importantkey.put("아미노산",1.0);
    		importantkey.put("아세트알데히드",1.0);
    		importantkey.put("아연",1.0);
    		importantkey.put("아이스크림",1.0);
    		importantkey.put("아크릴아마이드",1.0);
    		importantkey.put("아플라톡신",1.0);
    		importantkey.put("알루미늄",1.0);
    		importantkey.put("알코올",1.0);
    		importantkey.put("야채",1.0);
    		importantkey.put("양념",1.0);
    		importantkey.put("양파",1.0);
    		importantkey.put("어패류",1.0);
    		importantkey.put("에틸카바메이트",1.0);
    		importantkey.put("열매",1.0);
    		importantkey.put("오리고기",1.0);
    		importantkey.put("오메가3",1.0);
    		importantkey.put("오비맥주",1.0);
    		importantkey.put("오일",1.0);
    		importantkey.put("옥수수",1.0);
    		importantkey.put("외식",1.0);
    		importantkey.put("요리",1.0);
    		importantkey.put("요오드",1.0);
    		importantkey.put("우유",1.0);
    		importantkey.put("원료",1.0);
    		importantkey.put("원전사고",1.0);
    		importantkey.put("유기산",1.0);
    		importantkey.put("유산균",1.0);
    		importantkey.put("유제품",1.0);
    		importantkey.put("유해물질",1.0);
    		importantkey.put("유효성분음료",1.0);
    		importantkey.put("음식",1.0);
    		importantkey.put("음식물",1.0);
    		importantkey.put("음식점",1.0);
    		importantkey.put("이물질",1.0);
    		importantkey.put("이유식",1.0);
    		importantkey.put("인공향료",1.0);
    		importantkey.put("입맛",1.0);
    		importantkey.put("잔류농약",1.0);
    		importantkey.put("전성분",1.0);
    		importantkey.put("젓갈",1.0);
    		importantkey.put("조리",1.0);
    		importantkey.put("조미료",1.0);
    		importantkey.put("주성분",1.0);
    		importantkey.put("주요성분",1.0);
    		importantkey.put("주원료",1.0);
    		importantkey.put("중금속",1.0);
    		importantkey.put("지방",1.0);
    		importantkey.put("참외",1.0);
    		importantkey.put("채소",1.0);
    		importantkey.put("천연",1.0);
    		importantkey.put("천연성분",1.0);
    		importantkey.put("첨가물",1.0);
    		importantkey.put("체중",1.0);
    		importantkey.put("카페인",1.0);
    		importantkey.put("칼로리",1.0);
    		importantkey.put("칼륨",1.0);
    		importantkey.put("칼슘",1.0);
    		importantkey.put("캡슐",1.0);
    		importantkey.put("커피",1.0);
    		importantkey.put("코발트",1.0);
    		importantkey.put("콜라겐",1.0);
    		importantkey.put("콜레스테롤",1.0);
    		importantkey.put("콜타르",1.0);
    		importantkey.put("코롬",1.0);
    		importantkey.put("클로르피리포스",1.0);
    		importantkey.put("타르",1.0);
    		importantkey.put("탄수화물",1.0);
    		importantkey.put("탈크",1.0);
    		importantkey.put("탤크",1.0);
    		importantkey.put("토마토",1.0);
    		importantkey.put("톨투엔",1.0);
    		importantkey.put("트랜스지방",1.0);
    		importantkey.put("트리에탄올아민",1.0);
    		importantkey.put("트리클로산",1.0);
    		importantkey.put("파라벤",1.0);
    		importantkey.put("파라핀",1.0);
    		importantkey.put("판매금지",1.0);
    		importantkey.put("페녹시에탄올",1.0);
    		importantkey.put("포름알데히드",1.0);
    		importantkey.put("퓨란",1.0);
    		importantkey.put("프로폴리스",1.0);
    		importantkey.put("프로필파라벤",1.0);
    		importantkey.put("플로필렌글라이콜",1.0);
    		importantkey.put("학교급식",1.0);
    		importantkey.put("합성착색료",1.0);
    		importantkey.put("항산화",1.0);
    		importantkey.put("향료",1.0);
    		importantkey.put("홍삼",1.0);
    		importantkey.put("화학성분",1.0);
    		importantkey.put("환경호르몬",1.0);
    		importantkey.put("황색포도상구균",1.0);
    		importantkey.put("효소",1.0);
    		importantkey.put("히알루론산",1.0);
    		importantkey.put("견과류",1.0);
    		importantkey.put("고기",1.0);
    		importantkey.put("고사리",1.0);
    		importantkey.put("과일",1.0);
    		importantkey.put("농산물",1.0);
    		importantkey.put("당근",1.0);
    		importantkey.put("돼지고기",1.0);
    		importantkey.put("마늘",1.0);
    		importantkey.put("버섯",1.0);
    		importantkey.put("봄나물",1.0);
    		importantkey.put("사과",1.0);
    		importantkey.put("사탕수수",1.0);
    		importantkey.put("수박",1.0);
    		importantkey.put("수산물",1.0);
    		importantkey.put("식물",1.0);
    		importantkey.put("아이스크림",1.0);
    		importantkey.put("야채",1.0);
    		importantkey.put("양파",1.0);
    		importantkey.put("어패류",1.0);
    		importantkey.put("열매",1.0);
    		importantkey.put("오리고기",1.0);
    		importantkey.put("옥수수",1.0);
    		importantkey.put("우유",1.0);
    		importantkey.put("유제품",1.0);
    		importantkey.put("잔류농약",1.0);
    		importantkey.put("참외",1.0);
    		importantkey.put("채소",1.0);
    		importantkey.put("축산물",1.0);
    		importantkey.put("치즈",1.0);
    		importantkey.put("토마토",1.0);
    		importantkey.put("AI",1.0);
    		importantkey.put("조류독감",1.0);
    	}
    }
 
    /** 
    * @Method Name : wordSpaceAnalyze 
    * @변경이력              : 
    * @Method 설명     : 뛰어 쓰기
    * @param source
    * @param force
    * @return
    * @throws MorphException 
    */
    public String wordSpaceAnalyze(String source, boolean force) throws MorphException {
        WordSegmentAnalyzer wsAnal = new WordSegmentAnalyzer();
         
        StringBuilder result = new StringBuilder();
         
        String s;
        if (force)
            s = source.replace(" ", "");
        else
            s = source;
        List<List<AnalysisOutput>> outList = wsAnal.analyze(s);
        for (List<AnalysisOutput> o : outList) {
            for (AnalysisOutput analysisOutput : o) {
                result.append(analysisOutput.getSource()).append(" ");
            }
 
        }
 
        return result.toString();
    }
 
    /** 
    * @Method Name : wordSpaceAnalyze 
    * @변경이력              : 
    * @Method 설명     : 뛰어 쓰기
    * @param source
    * @return
    * @throws MorphException 
    */
    public String wordSpaceAnalyze(String source) throws MorphException {
        return wordSpaceAnalyze(source, false);
    }
 
    /** 
    * @Method Name : compoundNounAnalyze 
    * @변경이력              : 
    * @Method 설명     : 복합 명사 분해
    * @param source
    * @return
    * @throws MorphException 
    */
    
    public String compoundNounAnalyze(String source) throws MorphException {
        CompoundNounAnalyzer cnAnal = new CompoundNounAnalyzer(); // 복합어 분석기
         
        StringBuilder result = new StringBuilder();
         
        List<CompoundEntry> outList = cnAnal.analyze(source);
        for (CompoundEntry o : outList) {
            result.append(o.getWord()).append(" ");
        }
 
        return result.toString();
    }
 
    /** 
    * @Method Name : guideWord 
    * @변경이력              : 
    * @Method 설명     : 색인어 추출
    * @param source
    * @return
    * @throws MorphException 
    */
    public String guideWord(String source) throws MorphException {
        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
     
        StringTokenizer stok = new StringTokenizer(source, " "); // 쿼리문을 뛰어쓰기 기준으로 토큰화
         
        StringBuilder result = new StringBuilder();
         
        while (stok.hasMoreTokens()) {
             
            String token = stok.nextToken();
             
            List<AnalysisOutput> outList = maAnal.analyze(token);
            for (AnalysisOutput o : outList) {
                 
                result.append(o.getStem());
                 
                for (CompoundEntry s : o.getCNounList()) {
                        result.append("+" + s.getWord());
                }
             
                result.append(",");
            }
        }
        String s = result.toString();
        if (s.endsWith(","))
            s = s.substring(0, s.length() - 1);
        return s;
    } 
    
//    private Set <String> guideWord_restrict(String source) throws MorphException {
//        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
//     
//        StringTokenizer stok = new StringTokenizer(source, " "); // 쿼리문을 뛰어쓰기 기준으로 토큰화
//        Set <String> SentenceWordIndex = new HashSet<String>();
//        while (stok.hasMoreTokens()) 
//        {
//             
//            String token = stok.nextToken();
//            List<AnalysisOutput> outList = maAnal.analyze(token);
//            List<String> WordIndex = new ArrayList<String>();
//            for (AnalysisOutput o : outList) 
//            {
//            	WordIndex.add(o.getStem());
//                for (CompoundEntry s : o.getCNounList())
//                {
//                	WordIndex.add(s.getWord());
//                }
//            }
//            if(WordIndex.size() > 1)
//            	WordIndex.remove(0);
//            SentenceWordIndex.addAll(WordIndex);
//        }
//        return SentenceWordIndex;
//    }
    
    //형태로 분석기에서 영문은 별도 명사로 구분->구분자를 이용하여 추출.
    private List <String> guideWord_restrict(String source) throws MorphException {
        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
        source = source.replace(" ", ",");
        StringTokenizer stok = new StringTokenizer(source, ","); // 쿼리문을 뛰어쓰기 기준으로 토큰화
        Set <String> SentenceWordIndex = new HashSet<String>();
        List<String> WordIndex = new ArrayList<String>();
        while (stok.hasMoreTokens()) 
        {
             
            String token = stok.nextToken();
            List<AnalysisOutput> outList = maAnal.analyze(token);
            
            for (AnalysisOutput o : outList) 
            {
//            	WordIndex.add(o.getStem());
            	char stem = o.getPos();
            	if(stem == 'N')
            	{
//            		System.out.println("N:"+ o.getStem());
            		WordIndex.add(o.getStem());
            	}
            	else
            	{
	            	if(o.getCNounList().size() > 0)
	            	{
		                for (CompoundEntry s : o.getCNounList())
		                {
		                	WordIndex.add(s.getWord());
		                }
	            	}
            	}
            }
            
        }
        return WordIndex;
    }
    private boolean isStringDouble(String s) 
    {
        try
        {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
      }
    private boolean beNumberInString(String input)
    {
    	for(int vi = 0; vi < input.length(); vi++)
    	{
    		String value = input.substring(vi, vi+1);
    		if(isStringDouble(value) == true)
    		{
    			return true;
    		}
    		
    	}
    	return false;
    }
    public List<String> getMDFS_KeyWord(String documents)
    {
    	List<String> MDFS_KEYWORD = null;
    	try
    	{
    		documents = documents.replaceAll("\\[|\\]|\\(|\\)|\\▶|\\ⓒ|\\‘|\\,|\\’|\\.|\\-", " ");
    		String match = "[^\uAC00-\uD7A3xfe0-9a-zA-Z\\s]";
    		documents = documents.replaceAll(match, " ");		
    		//연속 스페이스 제거
    		String match2 = "\\s{2,}";
    		documents = documents.replaceAll(match2, " ");
    		documents = documents.replaceAll("\t", " ");
    		//Check MDFS_Keyword
    		String[] words = documents.split(" ");

    		boolean BExist = false;
    		MDFS_KEYWORD = new ArrayList<String>();
    		if(use_keyword == true )
    		{    			
    			//중요 단어 부분매칭 사용 1차 검출.
    			Iterator<String> keys = importantkey.keySet().iterator();
		        while( keys.hasNext() )
		        {
		            String key = keys.next();
		            
		            for(int wi =0; wi < words.length ; wi++)
		            {
		            	if(words[wi].indexOf(key) >= 0) 
		            	{
		            		BExist =  true;
		            		if(words[wi].length() >=3) //조사 제거.
		        			{
    		        			char char_josa = getLastChar(words[wi]);
    		        			String str_josa = String.valueOf(char_josa);
    		        			if(josaSet.contains(str_josa) == true)
    		        			{
    		        				int josa_idx = words[wi].lastIndexOf(str_josa);
    		        				words[wi] = words[wi].substring(0, josa_idx);
    		        			}
		        			}
		            		MDFS_KEYWORD.add(words[wi]);
		            	}
		            }
		        }
    		}
    		
    		if(use_keyword == true  & BExist == true)
    		{
//    			Set <String> docWord = this.guideWord_restrict(documents);
    			Set <String> docWord = extractNoun(documents);
    	    	if(docWord.size() > 0)
    			{
    		        for (String str_Token : docWord) 
    		        {
    		        	str_Token = str_Token.replaceAll("\\p{Z}", "");
    		        	str_Token = str_Token.replaceAll(" ", "");
    		        	if(str_Token != ""|| str_Token.length() > 0)
    		        	{
    		        		if(beNumberInString(str_Token) == false)
    		        		{
    		        			if(str_Token.length() >=3) //조사 제거.
    		        			{
	    		        			char char_josa = getLastChar(str_Token);
	    		        			String str_josa = String.valueOf(char_josa);
	    		        			if(josaSet.contains(str_josa) == true)
	    		        			{
	    		        				int josa_idx = str_Token.lastIndexOf(str_josa);
	    		        				str_Token = str_Token.substring(0, josa_idx);
	    		        			}
    		        			}
    		        			MDFS_KEYWORD.add(str_Token); //데이터추가.
    		        		}
    		        	}
    		        }
    			}
        		
    		}
    		else if(use_keyword == true  & BExist == false)
    		{
    			return MDFS_KEYWORD;
    		}
    		else
    		{ 
//    			Set <String> docWord = this.guideWord_restrict(documents);    	    	
//    	    	if(docWord.size() > 0)
//    			{
//    		        for (String str_Token : docWord) 
//    		        {
//    		        	str_Token = str_Token.replaceAll("\\p{Z}", "");
//    		        	str_Token = str_Token.replaceAll(" ", "");
//    		        	if(str_Token != ""|| str_Token.length() > 0)
//    		        	{
//    		        		if(beNumberInString(str_Token) == false)
//    		        		{
//		        				char char_josa = getLastChar(str_Token);
//    		        			String str_josa = String.valueOf(char_josa);
//    		        			if(josaSet.contains(str_josa) == true)
//    		        			{
//    		        				int josa_idx = str_Token.lastIndexOf(str_josa);
//    		        				str_Token = str_Token.substring(0, josa_idx);
//    		        			}
//    		        			
//    		        			MDFS_KEYWORD.add(str_Token); //데이터추가.
//    		        		}
//    		        	}
//    		        }
//    			}
    			List <String> docWord = this.guideWord_restrict(documents);    	    	
    	    	if(docWord.size() > 0)
    			{
    		        for (String str_Token : docWord) 
    		        {
    		        	str_Token = str_Token.replaceAll("\\p{Z}", "");
    		        	str_Token = str_Token.replaceAll(" ", "");
    		        	if(str_Token != ""|| str_Token.length() > 0)
    		        	{
    		        		if(beNumberInString(str_Token) == false)
    		        		{
		        				char char_josa = getLastChar(str_Token);
    		        			String str_josa = String.valueOf(char_josa);
    		        			if(josaSet.contains(str_josa) == true)
    		        			{
    		        				int josa_idx = str_Token.lastIndexOf(str_josa);
    		        				str_Token = str_Token.substring(0, josa_idx);
    		        			}
    		        			
    		        			MDFS_KEYWORD.add(str_Token); //데이터추가.
    		        		}
    		        	}
    		        }
    			}
    		}	
    	}
    	catch(Exception e)
    	{
    		return null;
    	}
		return MDFS_KEYWORD;
    }
    /** 
    * @Method Name : extractNoun 
    * @변경이력              : 
    * @Method 설명     : 명사 추출
    * @param searchQuery
    * @return
    * @throws MorphException 
    */
//    public ArrayList<String> extractNoun(String searchQuery) throws MorphException{
//        ArrayList<String> nounList = new ArrayList<String>();       
//         
//        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
//        StringTokenizer stok = new StringTokenizer(searchQuery, " "); // 쿼리문을 뛰어쓰기 기준으로 토큰화
//         
//        // 색인어 분석기를 통해 토큰에서 색인어 추출
//        while (stok.hasMoreTokens()) {
//            String token = stok.nextToken();
//             
//            // 형태소 분석
//            List<AnalysisOutput> indexList = maAnal.analyze(token);
//             
//            for (AnalysisOutput morpheme : indexList) 
//                // 명사 추출 
//                if(morpheme.getPos() == 'N')
//                    nounList.add(morpheme.getStem());
//        }
//         
//        return nounList;
//    }
    public Set<String> extractNoun(String searchQuery) throws MorphException{
    	Set<String> nounList = new HashSet<String>();       
         
        MorphAnalyzer maAnal = new MorphAnalyzer(); // 형태소 분석기 
        StringTokenizer stok = new StringTokenizer(searchQuery, " "); // 쿼리문을 뛰어쓰기 기준으로 토큰화
         
        // 색인어 분석기를 통해 토큰에서 색인어 추출
        while (stok.hasMoreTokens()) {
            String token = stok.nextToken();
             
            // 형태소 분석
            List<AnalysisOutput> indexList = maAnal.analyze(token);
             
            for (AnalysisOutput morpheme : indexList) 
                // 명사 추출 
                if(morpheme.getPos() == 'N')
                    nounList.add(morpheme.getStem());
        }
         
        return nounList;
    }
}