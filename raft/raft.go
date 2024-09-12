package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	log         []LogData

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	currentRole   string
	votedFor      int
	currentLeader int
	votesRecieved []int

	heartbeatCheckTimer int
	heartbeatReceived   bool
	applyCh             chan ApplyMsg
}

type LogData struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.currentRole == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistedTerm int
	var persistedVotedFor int
	var persistedLog []LogData
	var persistedcutOffIndex int

	if d.Decode(&persistedTerm) != nil || d.Decode(&persistedVotedFor) != nil || d.Decode(&persistedLog) != nil {
		log.Fatal("Error decoding")
	} else {
		rf.mu.Lock()
		rf.currentTerm = persistedTerm
		rf.votedFor = persistedVotedFor
		rf.log = persistedLog
		rf.cutOffIndex = persistedcutOffIndex
		rf.commitIndex = 0
		rf.lastApplied = 0
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
	Entries           []LogData
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = "follower"
		rf.votedFor = -1
	}

	rf.currentLeader = args.LeaderId
	reply.Term = rf.currentTerm

	fmt.Println("Before InstallSnapshot", rf.me, rf.log)
	if args.LastIncludedIndex > rf.cutOffIndex+len(rf.log)-1 {
		rf.log = make([]LogData, 0)
		rf.log = append(rf.log, LogData{Term: args.LastIncludedTerm, Command: nil})
	} else {
		rf.log = rf.log[args.LastIncludedIndex-rf.cutOffIndex:]
	}
	fmt.Println("After InstallSnapshot", rf.me, rf.log)

	// rf.log = args.Entries
	rf.cutOffIndex = args.LastIncludedIndex
	rf.lastApplied = rf.cutOffIndex

	fmt.Println("For server", rf.me, "cutoffIndex is", rf.cutOffIndex)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.cutOffIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	appMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.applyCh <- appMsg

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index > rf.lastApplied || index <= rf.cutOffIndex {
			return
		}

		rf.cutOffIndex = index
		fmt.Println("new cutoffindex is...", rf.cutOffIndex, "in", rf.me)
		rf.log = rf.log[index-rf.cutOffIndex:]
		rf.snapshotData = snapshot

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		e.Encode(rf.cutOffIndex)
		data := w.Bytes()

		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}()
	time.Sleep(50 * time.Millisecond)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		rf.persist()
		return
	}
	// --------------- QUESTION -----------------
	// If we need to check between log's term index, why does config not have it?
	// Is it ok to just look at the current term from the raft object itself?
	// Do I define my own logEntry?
	// Also what is the difference between Term and LastLogTerm?

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentRole = "follower"
	}

	rfLogTerm := rf.log[len(rf.log)-1].Term

	//log check from reading
	logCheck := (rfLogTerm > args.LastLogTerm) || (rfLogTerm == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)

	voteCheck := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	////(strconv.Itoa(args.LastLogTerm) + " " + strconv.Itoa(rfLogTerm))
	//("logCheck " + strconv.FormatBool(!logCheck) + " voteCheck " + strconv.FormatBool(voteCheck))
	if !logCheck && voteCheck {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.heartbeatReceived = true
		// //("vote granted")
	} else {
		// //("vote denied")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogData
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int
	ConflictingTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//("AppendEntries Called")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		rf.persist()
		return
	}
	rf.heartbeatReceived = true
	reply.Term = rf.currentTerm
	reply.Success = true

	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentRole = "follower"
	}
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = len(rf.log)
		reply.ConflictingTerm = -1
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictingTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; rf.log[i].Term == reply.ConflictingTerm; i-- {
			reply.ConflictingIndex = i
		}
		return
	}

	// if args.Term == rf.currentTerm {
	// 	rf.currentRole = "follower"
	// 	rf.currentLeader = args.LeaderId
	// }

	logOk := (len(rf.log) >= args.PrevLogIndex && (args.PrevLogIndex >= 1 || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm))
	// if args.PrevLogIndex != 0 {
	// 	// //("log == prev log term check")
	// 	// //(rf.log[args.PrevLogIndex-1].Term)
	// 	// //(args.PrevLogTerm)
	// 	// //(rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm)
	// } else {
	// 	// //("prev = 0 check")
	// 	// //(args.PrevLogIndex == 0)
	// }
	//fmt.//(rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	if rf.currentTerm == args.Term && logOk {
		rf.log = args.Entries
	} else {
		////("ASDASJIODASDAJSASDJOAISDJSAOJDAOIJDAOISDJIO -----------")
		reply.Success = false
		return
	}

	i := rf.commitIndex
	////("rf.commitIndex: " + strconv.Itoa(i))
	////("args.LeaderCommit: " + strconv.Itoa(args.LeaderCommit))
	for i < args.LeaderCommit && i < len(rf.log)-1 {
		//("follower " + strconv.Itoa(rf.me) + " is comitting to " + strconv.Itoa(i) + " because of " + strconv.Itoa(args.LeaderId))
		i++
		appMsg := ApplyMsg{
			Command:      rf.log[i].Command,
			CommandValid: true,
			CommandIndex: i,
		}
		rf.commitIndex = i
		rf.lastApplied = i
		rf.applyCh <- appMsg
	}
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if rf.killed() {
		return ok
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		//("KILLED!! :OOOO")
		return index, term, false
	}

	term, isLeader = rf.GetState()

	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	data := LogData{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, data)
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)
	//(command, "receieved in", rf.me)
	rf.persist()
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// //("election timeout!")
	//start election by becoming candidate
	rf.mu.Lock()
	rf.currentTerm++
	// //(rf.currentRole + " this was our PREV ROLE")
	rf.currentRole = "candidate"
	// //(rf.currentRole + " SWAPPED TO CANDIDATE")
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	totalVotes := 1

	for i := range rf.peers {

		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}

		//(strconv.Itoa(rf.me) + " is asking for vote from " + strconv.Itoa(i))
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {

				rf.mu.Lock()
				defer rf.mu.Unlock()
				//(strconv.Itoa(i) + " OK! " + strconv.Itoa(reply.Term) + " " + strconv.Itoa(rf.currentTerm))
				if reply.Term == rf.currentTerm && reply.VoteGranted {
					//("Vote granted! to " + strconv.Itoa(rf.me))
					totalVotes += 1
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentRole = "follower"
					rf.votedFor = -1
					rf.heartbeatReceived = true
					return
				}
				// //("rf.me: " + strconv.Itoa(rf.me))
				// //("rf.currentRole " + rf.currentRole)
				// //("rf.currentTerm " + strconv.Itoa(rf.currentTerm))
				// //("args.Term " + strconv.Itoa(args.Term))
				if rf.currentRole != "candidate" || rf.currentTerm != args.Term {
					return
				}

				if totalVotes >= (len(rf.peers)+1)/2 {
					// //("leader found")
					rf.currentRole = "leader"
					//(strconv.Itoa(rf.me) + " is leader!!!!")
					rf.nextIndex = make([]int, len(rf.peers))
					for i = 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = len(rf.log)
					}
					rf.matchIndex = make([]int, len(rf.peers))
				}
			}
		}(i)

	}
	time.Sleep(20 * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		currentRole := rf.currentRole
		rf.mu.Unlock()
		if currentRole == "follower" {
			time.Sleep(time.Duration(rf.heartbeatCheckTimer) * time.Millisecond)
			rf.mu.Lock()
			hbReceived := rf.heartbeatReceived
			rf.mu.Unlock()
			if hbReceived == false {
				rf.mu.Lock()
				rf.currentRole = "candidate"
				rf.mu.Unlock()
			} else {
				//("heartbeat checked in " + strconv.Itoa(rf.me))
				rf.heartbeatReceived = false
			}
		} else if currentRole == "candidate" {
			time.Sleep(time.Duration(rf.heartbeatCheckTimer) * time.Millisecond)

			rf.mu.Lock()
			hbReceived := rf.heartbeatReceived
			rf.mu.Unlock()
			if hbReceived == false {
				//("Starting election in " + strconv.Itoa(rf.me))
				rf.startElection()
			} else {
				rf.mu.Lock()
				rf.currentRole = "follower"
				rf.mu.Unlock()
				//("heartbeat checked in!!! " + strconv.Itoa(rf.me))
				rf.heartbeatReceived = false
				rf.heartbeatCheckTimer = rand.Intn(2000) + 1000
			}
		} else {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.heartbeatReceived {
				rf.currentRole = "follower"
				rf.mu.Unlock()
				break
			}

			time.Sleep(50 * time.Millisecond)
			//(strconv.Itoa(rf.me) + " thinks it is the leader! with term... " + strconv.Itoa(rf.currentTerm))
			rf.mu.Unlock()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.killed() {
					break
				}
				rf.mu.Lock()
				sendEntry := make([]LogData, len(rf.log))
				copy(sendEntry, rf.log)
				// //(rf.commitIndex)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					Entries:      sendEntry,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					LeaderCommit: rf.commitIndex,
				}

				reply := AppendEntriesReply{}
				rf.mu.Unlock()

				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					//("rf.nextIndex[" + strconv.Itoa(i) + "] = " + strconv.Itoa(rf.nextIndex[i]))
					//(strconv.FormatBool(reply.Success) + " success from " + strconv.Itoa(i))
					currentTerm := rf.currentTerm
					currentRole := rf.currentRole

					rf.mu.Unlock()

					if reply.Success && currentRole == "leader" {

						rf.mu.Lock()
						rf.matchIndex[i] = len(args.Entries) - 1
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.mu.Unlock()

					} else {
						if reply.Term > currentTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.currentRole = "follower"
							rf.votedFor = -1
							rf.heartbeatReceived = true
							rf.mu.Unlock()
							break
						} else if reply.Term == currentTerm {
							rf.nextIndex[i] = reply.ConflictingIndex
						} else if reply.ConflictingTerm != -1 {
							for j := args.PrevLogIndex; j >= 1; j-- {
								if rf.log[j-1].Term == reply.ConflictingTerm {
									// in next trial, check if log entries in ConflictTerm matches
									rf.nextIndex[i] = j
									break
								}
							}
						}
					}
				}
			}
			rf.mu.Lock()

			for i := rf.commitIndex + 1; i < len(rf.log); i++ {
				commitCount := 0
				for j := 0; j < len(rf.peers); j++ {
					if i <= rf.matchIndex[j] {
						commitCount++
					}
				}

				if commitCount >= (len(rf.peers)+1)/2 {
					rf.commitIndex = i
					//("leader " + strconv.Itoa(rf.me) + " committing: " + strconv.Itoa(rf.commitIndex))
					appMsg := ApplyMsg{
						Command:      rf.log[rf.commitIndex].Command,
						CommandValid: true,
						CommandIndex: rf.commitIndex,
					}
					rf.applyCh <- appMsg

				}
			}

			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogData, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.commitIndex = 0
	rf.currentRole = "follower"
	rf.currentLeader = -1
	rf.votesRecieved = []int{}
	rf.applyCh = applyCh
	dummy := ApplyMsg{
		CommandValid: true,
		Command:      0,
		CommandIndex: 0,
	}
	rf.applyCh <- dummy

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.heartbeatCheckTimer = rand.Intn(2000) + 1000
	// //(rf.heartbeatCheckTimer)
	go rf.ticker()

	return rf
}
