package pl.mproch.streaming.model;

public enum Topics {
    
    messages(Colors.ANSI_YELLOW),
    users(Colors.ANSI_BLACK),
    highRankUsers(Colors.ANSI_BLUE),
    messageCount(Colors.ANSI_CYAN),
    timedMessageCount(Colors.ANSI_GREEN),
    averageRateByText(Colors.ANSI_PURPLE),
    lowRatingUsers(Colors.ANSI_RED),
    lowRankMessages(Colors.ANSI_GREEN),
    usersPostingMessage(Colors.ANSI_RED);
    
    public String color;
    
    Topics(String color) {
        this.color = color;    
    }

    private static class Colors {
        private static final String ANSI_BLACK = "\u001B[30m";
        private static final String ANSI_RED = "\u001B[31m";
        private static final String ANSI_GREEN = "\u001B[32m";
        private static final String ANSI_YELLOW = "\u001B[33m";
        private static final String ANSI_BLUE = "\u001B[34m";
        private static final String ANSI_PURPLE = "\u001B[35m";
        private static final String ANSI_CYAN = "\u001B[36m";
    }
}
