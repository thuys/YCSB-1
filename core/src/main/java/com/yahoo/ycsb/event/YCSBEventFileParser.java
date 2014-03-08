package com.yahoo.ycsb.event;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class YCSBEventFileParser {
	private final String 	TAG_NAME = "event",
							ID_NAME = "id",
							START_EXECUTION_NAME = "startInMS";
	private final String file;

	public YCSBEventFileParser(String file) {
		super();
		this.file = file;
	}
	
	public Set<YCSBEvent> parse(){
		Set<YCSBEvent> set = new HashSet<YCSBEvent>();
		try{
		
		
		File fXmlFile = new File(file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		
		//optional, but recommended
		//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
		doc.getDocumentElement().normalize();
	 
		NodeList nList = doc.getElementsByTagName(TAG_NAME);
		 	 
		for (int temp = 0; temp < nList.getLength(); temp++) {
	 
			Node nNode = nList.item(temp);
	 	 
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	 
				try{
					Element eElement = (Element) nNode;
					YCSBEvent ycsbEvent = parseElement(eElement);
					set.add(ycsbEvent);
				}catch(Exception e){
					System.err.println("Exception in parsing: " + e);
				}
	 
			}
		}
			
		}catch(Exception e){ 
			System.err.println("Parse error: " + e);
		}
		return set;
	}
	
	private YCSBEvent parseElement(Element element){
		String id = element.getAttribute(ID_NAME);
		String startString = element.getAttribute(START_EXECUTION_NAME);
		String commands = element.getTextContent().trim();
		
		long start = Long.parseLong(startString);

		
		return new YCSBEvent(id, start, commands);
	}
	
	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
		String file = "D:/Schooljaar 2013-2014/Thesis/YCSB/test.xml";
		YCSBEventFileParser parser = new YCSBEventFileParser(file);
		parser.parse();
	}
}
