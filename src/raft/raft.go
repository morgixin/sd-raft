package raft

import (
	"sync"
	"labrpc"
	"math/rand"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	index int // indice no log
	term  int // term de quando a entrada foi recebida

}

type AppendEntry struct {
	LeaderId   int // id do lider que envia append message
	LeaderTerm int // termo do lider que envia append message
}

type AppendEntryReply struct {
	PeerTerm      int  // id do peer que recebe append message
	EntryReceived bool // confirmacao de que a mensagem foi recebida
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	
	currentTerm int             // ultimo termo que o servidor viu
	votedFor    int             // id do candidato que recebeu voto no termo atual

	role          Role          // diz o papel do servidor
	leader        int           // mantem o indice do lider atual (nao usado)
	numVotes      int           // mantem numero de votos recebidos
	electionTimer time.Duration // timer de eleicao do no
	timeout       time.Time     // tempo em que comeca o timer de eleicao do no
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	if rf.role == Leader {
		return rf.currentTerm, true
	}
	return rf.currentTerm, false
}

// funcao que simula o comportamento do lider, enviando heartbeats
// para todos os followers
func (rf *Raft) SendAppendEntries() {

	for {
		time.Sleep(time.Duration(50 * time.Millisecond)) // pausa de 50 milisegundos para enviar os heartbeats
		rf.mu.Lock()
		currentTerm := rf.currentTerm // salvando termo atual (caso mude em uma eleicao)
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			entry := AppendEntry{ // criando argumentos pra appendentry (mensagem enviada pelo lider)
				rf.me,
				currentTerm,
			}
			var reply AppendEntryReply // criando a reply que sera preenchida pelo follower

			go func() {
				// enviando os heartbeats em paralelo para os followers
				if rf.peers[i].Call("Raft.ReceiveEntry", &entry, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.PeerTerm > entry.LeaderTerm { // verificando se o termo do lider esta desatualizado, indicando inicio de outra eleicao
						// tornando o lider atual em follower e iniciando seu timer de eleicao
						rf.currentTerm = currentTerm
						rf.votedFor = -1
						rf.role = Follower
						rf.timeout = time.Now()
						go rf.CheckElectionTimers()
					}
				}
			}()
		}
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// funcao para monitorar os timers de eleicao de cada no
func (rf *Raft) CheckElectionTimers() {

	rf.mu.Lock()
	var currentTerm = rf.currentTerm // salvando termo atual do no para caso ele mude em outra eleicao
	rf.mu.Unlock()

	for {
		//verifica se o termo atual do no aumentou, indicando que ja ocorreu uma eleicao. verifica se o no ja se tornou lider. 
		rf.mu.Lock()
		if currentTerm < rf.currentTerm || rf.role == Leader { // verifica se o termo atual do no aumentou, indicando que ja ocorreu uma eleicao
			rf.mu.Unlock()
			return
		}

		if time.Since(rf.timeout) >= rf.electionTimer { // verifica se o timer de eleicao do no chegou ao fim
			rf.BeginElection() // o no inicia uma eleicao
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// iniciando uma eleicao
func (rf *Raft) BeginElection() {
	// incrementa o term do no, ele vira candidato e vota em si mesmo
	rf.currentTerm++
	var currentTerm = rf.currentTerm
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.numVotes++
	rf.timeout = time.Now()

	// lidando com as respostas dos peers
	HandleVoteResponse := func(reply RequestVoteReply, currentTerm int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != Candidate { // verificando se o no nao e mais candidato
			return
		}

		if reply.Term > currentTerm { // verifica se outra eleicao ocorreu.
			rf.timeout = time.Now()
			rf.votedFor = -1
			rf.role = Follower
			rf.currentTerm = currentTerm
			go rf.CheckElectionTimers()
			return
		} else if reply.VoteGranted && reply.Term == currentTerm { // caso o voto tenha sido concedido e os termos da reply e do candidato sejam iguais
			// numero de votos do no candidato aumenta
			rf.numVotes++
			if rf.numVotes > len(rf.peers)/2 { // verifica se o no possui a maioria dos votos
				// se possuir, vira lider e comeca a enviar heartbeatts
				rf.role = Leader
				go rf.SendAppendEntries() // funcao para enviar heartbeats paralelamente
				return
			}

		}

	}

	// enviando pedidos de votos aos peers
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		// envia os pedidos de voto paralelamente
		go func(no int) {
			// criando argumentos do pedido de voto
			args := RequestVoteArgs{
				CandidateId:  rf.me,
				Term:         currentTerm,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}
			var reply RequestVoteReply // criando reply a ser preenchida pelo follower

			// enviando RPC para os peers, se bem sucedido entra no if
			if rf.peers[no].Call("Raft.RequestVote", &args, &reply) {
				HandleVoteResponse(reply, currentTerm) // chama funcao para lidar com a resposta do follower
			}

		}(i)

	}

	go rf.CheckElectionTimers() // reinicia monitoramento de timer para caso haja problema com eleição atual

}

func (rf *Raft) Persist() {
	// Your code here (2C).
}

func (rf *Raft) ReadPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term         int // term do candidato
	CandidateId  int // id do candidato que requisita o voto
	LastLogIndex int // indice da ultima entrada no log do candidato
	LastLogTerm  int // term da ultima entrada no log do candidato
}

type RequestVoteReply struct {
	Term        int  // termo atual para que o candidato se atualize
	VoteGranted bool // se verdadeiro, indica que o candidato recebeu voto
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// definindo no como novo follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.timeout = time.Now()
		go rf.CheckElectionTimers()
	}

	if rf.votedFor == -1 && rf.currentTerm == args.Term {
		reply.VoteGranted = true       // indica que o voto foi concedido
		rf.votedFor = args.CandidateId // no votou no candidato
		rf.timeout = time.Now()        // reseta o timer do no follower
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm // atualiza o termo da reply
	return
}

func (rf *Raft) ReceiveEntry(entry *AppendEntry, reply *AppendEntryReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.EntryReceived = false

	if entry.LeaderTerm > rf.currentTerm {
		rf.currentTerm = entry.LeaderTerm
		rf.votedFor = -1
		rf.role = Follower
		rf.timeout = time.Now()
		go rf.CheckElectionTimers()
	} else if entry.LeaderTerm == rf.currentTerm { // caso o termo seja igual
		// verifica se o no nao e o proprio lider e se seu papel e candidato ou lider
		if rf.me != entry.LeaderId && rf.role != Follower {
			rf.currentTerm = entry.LeaderTerm
			rf.votedFor = -1
			rf.role = Follower
			rf.timeout = time.Now()

			go rf.CheckElectionTimers()
		}
		reply.EntryReceived = true // indica recebimento da append message (heartbeat confirma que lider esta vivo)
		rf.timeout = time.Now()    // reseta timer do no que recebeu a mensagem
	}
	reply.PeerTerm = rf.currentTerm // atualiza termo da reply
	return
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.numVotes = 0

	rf.timeout = time.Now()
	rf.electionTimer = time.Duration(150+rand.Intn(150)) * time.Millisecond

	go rf.CheckElectionTimers() // coroutine

	// initialize from state persisted before a crash
	rf.ReadPersist(persister.ReadRaftState())

	return rf
}
