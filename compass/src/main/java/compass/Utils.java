package compass;

enum MessageType {DATA, QUERY, DELETION, SAMPLE, EXIT, SAVE, TICK, RNDELETE};

class TopicMessage{
    MessageType messageType;
    String message;
    public TopicMessage(MessageType t, String s){
        messageType = t;
        message = s;
    }
    public String getMessage(){
        return message;
    }
    public MessageType getMessageType(){
        return messageType;
    }
    public String toString(){
        return messageType + " " + message;
    }
}
