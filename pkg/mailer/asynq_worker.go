package mailer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
	"github.com/matheusbanqueiro/gocommerce-tools/pkg/tasks"
)

// StartAsynqEmailServer inicia o servidor de processamento de tarefas do Asynq em background
func StartAsynqEmailServer(redisAddr string, concurrency int) *asynq.Server {

	// Se não receber concurrency, define o valor padrão como 10.
	// Essa concurreny é o número de workers que processarão as tarefas em paralelo.
	if concurrency <= 0 {
		concurrency = 10
	}

	// Aqui configuramos o servidor Asynq com as opções desejadas, incluindo a concorrência e as filas de prioridade.
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			Concurrency: concurrency,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				log.Printf("[Asynq Error] Erro ao processar tarefa %s: %v\n", task.Type(), err)
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypePasswordResetEmail, HandlePasswordResetEmailTask)

	go func() {
		log.Printf("[Asynq Worker - gocommerce-utils] Servidor de tarefas iniciado no Redis (%s) com %d workers concorrentes...\n", redisAddr, concurrency)
		if err := srv.Run(mux); err != nil {
			log.Printf("[Asynq Worker] Servidor finalizado: %v\n", err)
		}
	}()

	return srv
}

// HandlePasswordResetEmailTask processa a tarefa de envio de email de redefinição de senha
func HandlePasswordResetEmailTask(ctx context.Context, t *asynq.Task) error {
	var payload tasks.PasswordResetPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("falha ao deserializar payload da tarefa: %w", err)
	}

	log.Printf("\n======================================================================\n")
	log.Printf("[Asynq Worker | gocommerce-utils] 📧 SIMULAÇÃO DE ENVIO DE E-MAIL VIA ASYNQ\n")
	log.Printf("Para: %s <%s>\n", payload.UserName, payload.Email)
	log.Printf("Assunto: Sua Nova Senha de Acesso ao GoCommerce\n")
	log.Printf("Mensagem: Olá %s, sua nova senha gerada é: [%s]\n", payload.UserName, payload.TemporaryPassword)
	log.Printf("Utilize esta senha para realizar o seu login na plataforma.\n")
	log.Printf("======================================================================\n\n")

	return nil
}
